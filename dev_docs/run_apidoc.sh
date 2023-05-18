#!/usr/bin/bash
#sphinx-apidoc --ext-autodoc -e -M -o source/_autodoc ../raftframe ../raftframe/*/tests ../*.py
cd ..
sphinx-apidoc --ext-autodoc -e -M -o dev_docs/source/_autodoc dev_tools 
sphinx-apidoc --ext-autodoc -e -M -o dev_docs/source/_autodoc_bt bank_teller

