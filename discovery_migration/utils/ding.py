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

def dingdingding():
    try:
        #sfx = os.path.join(os.path.dirname(__file__), 'ding.wav')
        sfx = 'builder_pipe/utils/ding.wav'

        playsound(sfx)
        playsound(sfx)
        playsound(sfx)
    except:
        print("---------------------------------------\nDONE!")


if __name__ == "__main__":
    dingdingding()
