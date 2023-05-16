#!/usr/bin/bash
#sphinx-apidoc --ext-autodoc -e -M -o source/_autodoc ../raftframe ../raftframe/*/tests ../*.py
cd ..
sphinx-apidoc --ext-autodoc -e -M -o docs/source/_autodoc raftframe raftframe/*/tests 

