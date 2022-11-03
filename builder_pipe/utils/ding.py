import os.path

try:
    from playsound import playsound
except:
    pass


def ding():
    try:
        playsound(os.path.join(os.path.dirname(__file__), 'ding.wav'))
    except:
        print("---------------------------------------\nDONE!")
