pip uninstall -y lim
python setup.py bdist_wheel
pip install dist\lim-1.1.0-py2.py3-none-any.whl
REM pip install git+https://github.com/aeorxc/lim#egg=lim