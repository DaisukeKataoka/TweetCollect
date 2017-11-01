import json

def output_json(js, name):
    with open("{}.json".format(name), "w") as wf:
        json.dump(js, wf, 
                    ensure_ascii=False, 
                    indent=4, 
                    sort_keys=True, 
                    separators=(',', ': '))
    return 1