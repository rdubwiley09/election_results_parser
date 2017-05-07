import hug

@hug.get(examples="year=2016")
def get(year: hug.types.text, hug_timer=3):
    """Demo route"""
    return {"message": "you chose year: %s" %(year)}
