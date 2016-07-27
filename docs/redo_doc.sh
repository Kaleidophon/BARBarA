make clean;
rm modules.rst;
rm src*.rst;
sphinx-apidoc -o ./ ../
make html
make latexpdf
