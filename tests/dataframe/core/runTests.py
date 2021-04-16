import pytest
import os

if __name__ == '__main__':
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    print("cur_dir", cur_dir)
    pytest.main(cur_dir)
