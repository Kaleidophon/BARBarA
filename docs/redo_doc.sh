make clean;
rm modules.rst;
rm src*.rst;
sphinx-apidoc -o ./ ../src/
make html
make latexpdf
