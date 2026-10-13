package refs

import (
	"encoding/binary"
	"strings"

	"golang.org/x/crypto/sha3"
)

type Kind uint8

const (
	KindOther   Kind = 0
	KindUpDown  Kind = 1
	KindUnknown Kind = 2
)

const upDownPhrase = "up or down"

var NegRiskAdapter = [20]byte{
	0xd9, 0x1e, 0x80, 0xcf, 0x2e, 0x7b, 0xe2, 0xe1, 0x62, 0xc6,
	0x51, 0x3c, 0xed, 0x06, 0xf1, 0xdd, 0x0d, 0xa3, 0x52, 0x96,
}

func QuestionID(market [32]byte, q uint8) [32]byte {
	market[31] = q
	return market
}

func ConditionID(oracle [20]byte, question [32]byte) [32]byte {
	var in [84]byte
	copy(in[:20], oracle[:])
	copy(in[20:52], question[:])
	in[83] = 2
	h := sha3.NewLegacyKeccak256()
	h.Write(in[:])
	var out [32]byte
	h.Sum(out[:0])
	return out
}

func KindOf(question string) Kind {
	if question == "" {
		return KindUnknown
	}
	if strings.Contains(strings.ToLower(question), upDownPhrase) {
		return KindUpDown
	}
	return KindOther
}

func Bucket(eventKey [32]byte, n int) uint8 {
	return uint8(binary.BigEndian.Uint64(eventKey[:8]) % uint64(n))
}
