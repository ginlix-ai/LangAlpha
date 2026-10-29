#!/bin/sh
# Connects this machine to PRODUCT, which then works on it over SSH as the
# account running this script. It adds one public key to that account's
# ~/.ssh/authorized_keys and sends back what PRODUCT needs to reach the
# machine: the account name, the machine's name, its public SSH host keys,
