import os
import shutil
import numpy as np


def cwd_cleanup(func):
    def inner(*args):
        directory = "dir" + str(np.random.randint(10000, 99999))
        os.mkdir(directory)
        os.chdir(directory)
        try:
            return_value = func(*args)
        except Exception as error:
            raise error
        finally:
            os.chdir("../")
            shutil.rmtree(directory)
        return return_value

    return inner
