from typing import Any, NoReturn, Callable, Dict, List
import tempfile
import os


class Utils(object):
    @staticmethod
    def save() -> str:
        import base64
        import uuid
        import matplotlib.pyplot as plt
        plt.savefig(os.path.join(tempfile.gettempdir(), str(uuid.uuid4()) + ".png"))
        with open("/tmp/a.png", mode='rb') as file:
            fileContent = base64.b64encode(file.read()).decode()
