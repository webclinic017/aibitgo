import importlib.util

from base.config import BASE_DIR


def get_strategy_class(strategy_name: str):
    spec = importlib.util.spec_from_file_location(strategy_name, f"{BASE_DIR}/strategy/{strategy_name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    robot_strategy = getattr(module, strategy_name)
    return robot_strategy

# strategy = get_strategy_class('FeigeBollStrat1egy')
# print(strategy.__dict__)
