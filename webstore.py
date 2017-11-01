import json

def read_json(path):
    with open(path, "r") as rf:
        data = json.load(rf)
    return data

def store_webdata(data):
    pass

if __name__ == "__main__":
    l = ["学生_1.json", "社会人_1.json"]
    read_json(l[0])
    store_webdata()