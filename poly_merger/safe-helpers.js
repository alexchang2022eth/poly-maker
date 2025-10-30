const { BigNumber, ethers } = require('ethers');

// Simple sleep helper for backoff
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// Build EIP-1559 fee overrides (fallbacks to legacy gasPrice if needed)
async function ensureEip1559Overrides(provider, overrides = {}, priorityFeeGwei = 30) {
    const cloned = { ...overrides };
    // If caller already provided EIP-1559 fields, keep them
    if (cloned.maxFeePerGas && cloned.maxPriorityFeePerGas) {
        return cloned;
    }

    const feeData = await provider.getFeeData();
    // When the network supports EIP-1559, use its suggested values
    if (feeData.maxFeePerGas && feeData.maxPriorityFeePerGas) {
        cloned.maxFeePerGas = feeData.maxFeePerGas;
        cloned.maxPriorityFeePerGas = feeData.maxPriorityFeePerGas;
        // Remove legacy gasPrice if present to avoid conflicts
        delete cloned.gasPrice;
        return cloned;
    }

    // Fallback: construct fees from legacy gasPrice or a sensible default
    const baseGasPrice = feeData.gasPrice || ethers.utils.parseUnits('50', 'gwei');
    const priority = ethers.utils.parseUnits(String(priorityFeeGwei), 'gwei');
    // Approximate maxFeePerGas = base + priority
    cloned.maxPriorityFeePerGas = priority;
    cloned.maxFeePerGas = baseGasPrice.add(priority);
    delete cloned.gasPrice;
    return cloned;
}

// Bump EIP-1559 fees by a multiplier for replacement strategy
function bumpFees(feeOverrides, multiplier = 1.2) {
    const bump = (bn) => bn.mul(Math.round(multiplier * 100)).div(100);
    const bumped = { ...feeOverrides };
    if (bumped.maxFeePerGas) {
        bumped.maxFeePerGas = bump(bumped.maxFeePerGas);
    }
    if (bumped.maxPriorityFeePerGas) {
        bumped.maxPriorityFeePerGas = bump(bumped.maxPriorityFeePerGas);
    }
    // Ensure no legacy gasPrice sneaks back in
    delete bumped.gasPrice;
    return bumped;
}

// Identify retryable send errors that benefit from fee bump or nonce refresh
function isRetryableSendError(error) {
    const msg = String(error && error.message || '').toLowerCase();
    return (
        msg.includes('replacement fee too low') ||
        msg.includes('nonce too low') ||
        msg.includes('nonce has already been used') ||
        msg.includes('underpriced') ||
        msg.includes('timeout') ||
        msg.includes('rate limit') ||
        msg.includes('transaction was replaced') ||
        msg.includes('failed to meet minimum gas price')
    );
}

function joinHexData(hexData) {
    return `0x${hexData
        .map(hex => {
            const stripped = hex.replace(/^0x/, "");
            return stripped.length % 2 === 0 ? stripped : "0" + stripped;
        })
        .join("")}`;
}

function abiEncodePacked(...params) {
    return joinHexData(
        params.map(({ type, value }) => {
            const encoded = ethers.utils.defaultAbiCoder.encode([type], [value]);

            if (type === "bytes" || type === "string") {
                const bytesLength = parseInt(encoded.slice(66, 130), 16);
                return encoded.slice(130, 130 + 2 * bytesLength);
            }

            let typeMatch = type.match(/^(?:u?int\d*|bytes\d+|address)\[\]$/);
            if (typeMatch) {
                return encoded.slice(130);
            }

            if (type.startsWith("bytes")) {
                const bytesLength = parseInt(type.slice(5));
                return encoded.slice(2, 2 + 2 * bytesLength);
            }

            typeMatch = type.match(/^u?int(\d*)$/);
            if (typeMatch) {
                if (typeMatch[1] !== "") {
                    const bytesLength = parseInt(typeMatch[1]) / 8;
                    return encoded.slice(-2 * bytesLength);
                }
                return encoded.slice(-64);
            }

            if (type === "address") {
                return encoded.slice(-40);
            }

            throw new Error(`unsupported type ${type}`);
        })
    );
}

async function signTransactionHash(signer, message) {
    const messageArray = ethers.utils.arrayify(message);
    let sig = await signer.signMessage(messageArray);
    let sigV = parseInt(sig.slice(-2), 16);

    switch (sigV) {
        case 0:
        case 1:
            sigV += 31;
            break;
        case 27:
        case 28:
            sigV += 4;
            break;
        default:
            throw new Error("Invalid signature");
    }

    sig = sig.slice(0, -2) + sigV.toString(16);

    return {
        r: BigNumber.from("0x" + sig.slice(2, 66)).toString(),
        s: BigNumber.from("0x" + sig.slice(66, 130)).toString(),
        v: BigNumber.from("0x" + sig.slice(130, 132)).toString(),
    };
}

async function signAndExecuteSafeTransaction(signer, safe, to, data, overrides = {}, options = {}) {
    const nonce = await safe.nonce();
    console.log("Nonce for safe: ", nonce);
    const value = "0";
    const safeTxGas = "0";
    const baseGas = "0";
    const gasPrice = "0"; // Safe internal gas refund field; keep at 0
    const gasToken = ethers.constants.AddressZero;
    const refundReceiver = ethers.constants.AddressZero;
    const operation = 0;

    const txHash = await safe.getTransactionHash(
        to,
        value,
        data,
        operation,
        safeTxGas,
        baseGas,
        gasPrice,
        gasToken,
        refundReceiver,
        nonce
    );
    console.log("Transaction hash: ", txHash);

    const rsvSignature = await signTransactionHash(signer, txHash);
    const packedSig = abiEncodePacked(
        { type: "uint256", value: rsvSignature.r },
        { type: "uint256", value: rsvSignature.s },
        { type: "uint8", value: rsvSignature.v }
    );

    const provider = signer.provider;
    const priorityFeeGwei = options.priorityFeeGwei || (process.env.PRIORITY_FEE_GWEI ? Number(process.env.PRIORITY_FEE_GWEI) : 30);
    // Ensure we use EIP-1559
    let feeOverrides = await ensureEip1559Overrides(provider, overrides, priorityFeeGwei);
    // Default a large gasLimit if caller didn't specify (Safe calls can be heavy)
    if (!feeOverrides.gasLimit) {
        feeOverrides.gasLimit = overrides.gasLimit || 10_000_000;
    }

    const maxRetries = options.maxRetries || 3;
    const backoffMs = options.backoffMs || 2000;
    const bumpMultiplier = options.bumpMultiplier || 1.2;

    console.log("Executing transaction with EIP-1559 overrides:", {
        maxFeePerGas: feeOverrides.maxFeePerGas && feeOverrides.maxFeePerGas.toString(),
        maxPriorityFeePerGas: feeOverrides.maxPriorityFeePerGas && feeOverrides.maxPriorityFeePerGas.toString(),
        gasLimit: feeOverrides.gasLimit && feeOverrides.gasLimit.toString()
    });

    let attempt = 0;
    let lastError = null;
    while (attempt < maxRetries) {
        try {
            const txResponse = await safe.execTransaction(
                to,
                value,
                data,
                operation,
                safeTxGas,
                baseGas,
                gasPrice,
                gasToken,
                refundReceiver,
                packedSig,
                feeOverrides
            );
            return txResponse;
        } catch (err) {
            lastError = err;
            console.warn(`Safe exec attempt ${attempt + 1} failed:`, err && err.message || err);
            if (!isRetryableSendError(err)) {
                break;
            }
            // Bump fees and retry after backoff
            feeOverrides = bumpFees(feeOverrides, bumpMultiplier);
            await sleep(backoffMs * (attempt + 1));
        }
        attempt += 1;
    }

    // If we get here, all attempts failed
    throw lastError || new Error('Safe execTransaction failed with unknown error');
}

module.exports = {
    signAndExecuteSafeTransaction,
};