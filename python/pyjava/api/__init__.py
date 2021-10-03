from typing import Any, NoReturn, Callable, Dict, List
import tempfile
import os


class Utils(object):
    @staticmethod
    def show_plt() -> str:
        import base64
        import uuid
        import matplotlib.pyplot as plt
        img_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()) + ".png")
        plt.savefig(img_path)
        with open(img_path, mode='rb') as file:
            file_content = base64.b64encode(file.read()).decode()
        return file_content
