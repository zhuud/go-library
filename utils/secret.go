package utils

import (
    "crypto/hmac"
    "crypto/md5"
    "crypto/sha256"
    "encoding/base64"
    "encoding/hex"
    "hash/crc32"
)

//	Md5(s string) string ：returns the MD5 hash of the given string.
//	Sha256(s, salt string) string ：returns the SHA256 HMAC hash of the given string using the provided salt.
//	Crc32(s string) uint32 ：returns the CRC32 checksum of the given string.
//	XorEncrypt(input, key string) string ：performs XOR encryption on the input string using the given key and returns the Base64 encoded result.
//	XorDecrypt(encryptedBase64 string, key string) (string, error) ：performs XOR decryption on the Base64 encoded encrypted string using the given key.

func Md5(s string) string {
    h := md5.New()
    h.Write([]byte(s))
    return hex.EncodeToString(h.Sum(nil))
}

func Sha256(s, salt string) string {
    h := hmac.New(sha256.New, []byte(salt))
    h.Write([]byte(s))
    return hex.EncodeToString(h.Sum(nil))
}

func Crc32(s string) uint32 {
    return crc32.ChecksumIEEE([]byte(s))
}

// XorEncrypt XOR 对给定字符串使用给定密钥进行加密或解密，并返回Base64编码后的字符串
func XorEncrypt(input, key string) string {
    outputBytes := []byte(input)
    keyBytes := []byte(key)
    keyLen := len(keyBytes)

    for i := range outputBytes {
        outputBytes[i] ^= keyBytes[i%keyLen]
    }

    return base64.StdEncoding.EncodeToString(outputBytes)
}

// XorDecrypt XOR 使用给定密钥对Base64编码后的加密字符串进行解密
func XorDecrypt(encryptedBase64 string, key string) (string, error) {
    cipherBytes, err := base64.StdEncoding.DecodeString(encryptedBase64)
    if err != nil {
        return "", err
    }

    keyBytes := []byte(key)
    keyLen := len(keyBytes)

    for i := range cipherBytes {
        cipherBytes[i] ^= keyBytes[i%keyLen]
    }

    return string(cipherBytes), nil
}
