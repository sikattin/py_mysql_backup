#!/usr/bin/python3
import codecs
from getpass import getpass

def encrypt_string(line: str):
    encrypted = codecs.encode(line, 'rot_13')
    return encrypted

if __name__ == '__main__':
    plain_text = getpass("Input string you want to encrypt: ")
    encrypted_str = encrypt_string(plain_text)
    print("Before encription: {}".format(plain_text))
    print("After encription: {}".format(encrypted_str))
