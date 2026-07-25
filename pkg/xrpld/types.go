package xrpld

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	responseStatusSuccess = "success"

	streamValidations = "validations"
	streamLedger      = "ledger"

	messageTypeResponse           = "response"
	messageTypeValidationReceived = "validationReceived"
	messageTypeLedgerClosed       = "ledgerClosed"
)

var (
	ErrStreamAlreadyOrBeingSubscribed = errors.New("stream is already subscribed or being subscribed")

	// ErrFatal means that the Client is no longer usable.
	ErrFatal = errors.New("fatal error")
)

// MessageValidationReceived is the schema of a message received through the "validation" stream.
// See: https://xrpl.org/docs/references/http-websocket-apis/public-api-methods/subscription-methods/subscribe#validations-stream
type MessageValidationReceived struct {
	// The value validationReceived indicates this is from the validations stream.
	Type string `json:"type"`
	// (May be omitted) The amendments this server wants to be added to the protocol.
	Amendments []string `json:"amendments,omitempty"`
	// (May be omitted) The unscaled transaction cost (reference_fee value) this server
	// wants to set by Fee Voting.
	BaseFee int `json:"base_fee,omitempty"`
	// (May be omitted) An arbitrary value chosen by the server at startup. If the same
	// validation key pair signs validations with different cookies concurrently, that
	// usually indicates that multiple servers are incorrectly configured to use the same
	// validation key pair.
	Cookie any `json:"cookie,omitempty"`
	// Bit-mask of flags added to this validation message. The flag 0x80000000 indicates
	// that the validation signature is fully-canonical. The flag 0x00000001 indicates
	// that this is a full validation; otherwise it's a partial validation. Partial
	// validations are not meant to vote for any particular ledger. A partial validation
	// indicates that the validator is still online but not keeping up with consensus.
	Flags uint32 `json:"flags"`
	// If true, this is a full validation. Otherwise, this is a partial validation.
	// Partial validations are not meant to vote for any particular ledger. A partial validation
	// indicates that the validator is still online but not keeping up with consensus.
	Full bool `json:"full"`
	// The identifying hash of the proposed ledger is being validated.
	LedgerHash string `json:"ledger_hash"`
	// The Ledger Index of the proposed ledger.
	LedgerIndex string `json:"ledger_index"`
	// (May be omitted) The local load-scaled transaction cost this validator is currently enforcing,
	// in fee units.
	LoadFee int `json:"load_fee,omitempty"`
	// (May be omitted) The validator's master public key, if the validator is using a validator
	// token, in the XRP Ledger's base58 format. (See also: Enable Validation on your xrpld Server.)
	MasterKey string `json:"master_key,omitempty"`
	// (May be omitted) The minimum reserve requirement (account_reserve value) this validator wants
	// to set by Fee Voting.
	ReserveBase int `json:"reserve_base,omitempty"`
	// (May be omitted) The increment in the reserve requirement (owner_reserve value) this validator
	// wants to set by Fee Voting.
	ReserveInc int `json:"reserve_inc,omitempty"`
	// (May be omitted) An 64-bit integer that encodes the version number of the validating server.
	// For example, "1745990410175512576". Only provided once every 256 ledgers.
	ServerVersion string `json:"server_version,omitempty"`
	// The signature that the validator used to sign its vote for this ledger.
	Signature string `json:"signature"`
	// When this validation vote was signed, in seconds since the XRPL Epoch.
	SigningTime uint64 `json:"signing_time"`
	// The unique hash of the proposed ledger this validation applies to.
	ValidatedHash string `json:"validated_hash"`
	// The public key from the key-pair that the validator used to sign the message, in the XRP
	// Ledger's base58 format. This identifies the validator sending the message and can also be
	// used to verify the signature. If the validator is using a token, this is an ephemeral
	// public key.
	ValidationPublicKey string `json:"validation_public_key"`
}

// MessageLedgerClosed is the schema of a message received through the "ledger" stream.
// See: https://xrpl.org/docs/references/http-websocket-apis/public-api-methods/subscription-methods/subscribe#ledger-stream
type MessageLedgerClosed struct {
	// `ledgerClosed` indicates this is from the ledger stream
	Type string `json:"type"`
	// The reference transaction cost as of this ledger version, in drops of XRP. If this
	// ledger version includes a SetFee pseudo-transaction the new transaction cost applies
	// starting with the following ledger version.
	FeeBase int `json:"fee_base"`
	// (May be omitted) The reference transaction cost in "fee units". If the XRPFees
	// amendment is enabled, this field is permanently omitted as it will no longer be relevant.
	FeeRef int `json:"fee_ref"`
	// The identifying hash of the ledger version that was closed.
	LedgerHash string `json:"ledger_hash"`
	// The ledger index of the ledger that was closed.
	LedgerIndex any `json:"ledger_index"`
	// The time this ledger was closed, in seconds since the Ripple Epoch.
	LedgerTime uint64 `json:"ledger_time"`
	// The XRPL network of this stream.
	NetworkID int64 `json:"network_id"`
	// The minimum reserve, in drops of XRP, that is required for an account. If this ledger
	// version includes a SetFee pseudo-transaction the new base reserve applies starting with
	// the following ledger version.
	ReserveBase uint `json:"reserve_base"`
	// The owner reserve for each object an account owns in the ledger, in drops of XRP. If
	// the ledger includes a SetFee pseudo-transaction the new owner reserve applies after
	// this ledger.
	ReserveInc uint `json:"reserve_inc"`
	// Number of new transactions included in this ledger version.
	TxnCount int `json:"txn_count"`
	// (May be omitted) Range of ledgers that the server has available. This may be a disjoint
	// sequence such as 24900901-24900984,24901116-24901158. This field is not returned if the
	// server is not connected to the network, or if it is connected but has not yet obtained
	// a ledger from the network.
	ValidatedLedgers string `json:"validated_ledgers,omitempty"`
}

// LedgerIndexParsed parses the ledger index field to int.
func (m MessageLedgerClosed) LedgerIndexParsed() (int64, error) {
	switch x := m.LedgerIndex.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case string:
		parsed, err := strconv.Atoi(x)
		if err != nil {
			return 0, fmt.Errorf("invalid number: %s", x)
		}
		return int64(parsed), nil
	default:
		return 0, fmt.Errorf("unrecognized type: %v (%T)", x, x)
	}
}

// subscriptionRequest is the schema of a subscription request for xrpld.
type subscriptionRequest struct {
	ID      any      `json:"id"`
	Command string   `json:"command"`
	Streams []string `json:"streams"`
}

// subscriptionResponse is the schema of the response that xrpld gives for a subscription request.
type subscriptionResponse struct {
	ID     any    `json:"id"`
	Status string `json:"status"`
	Type   string `json:"type"`
	Result any    `json:"result"`
}
