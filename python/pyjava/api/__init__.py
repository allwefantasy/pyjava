from typing import Any, NoReturn, Callable, Dict, List
import tempfile
import os

from pyjava.api.mlsql import PythonContext


class Utils(object):

    @staticmethod
    def show_plt(plt: Any, context: PythonContext) -> NoReturn:
        content = Utils.gen_img(plt)
        context.build_result([{"content": content, "mime": "image"}])

    @staticmethod
    def gen_img(plt: Any) -> str:
        import base64
        import uuid
        img_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()) + ".png")
        plt.savefig(img_path)
        with open(img_path, mode='rb') as file:
            file_content = base64.b64encode(file.read()).decode()
        return file_content
