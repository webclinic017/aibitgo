import json
from datetime import datetime, date


class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


class Json:
    @staticmethod
    def dumps(data):
        return json.dumps(data, cls=CJsonEncoder)
