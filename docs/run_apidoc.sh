#!/usr/bin/bash
sphinx-apidoc --ext-autodoc -e -M -o source/_autodoc ../raft ../raft/*/tests ../*.py
