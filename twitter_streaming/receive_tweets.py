import json
import socket

from tweepy import Stream
from tweepy.auth import OAuthHandler
from tweepy.streaming import StreamListener

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

WORDS_TRACKED = ['gouvernement', 'macron', 'coronavirus', 'confinement', 'COVID19']


class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            message = json.loads(data)
            print(message['text'].encode('utf-8'))
            self.client_socket.send(message['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(
        track=WORDS_TRACKED)


if __name__ == "__main__":
    new_skt = socket.socket()
    host = ''
    port = 9009
    new_skt.bind((host, port))

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)
    c, addr = new_skt.accept()

    print("Received request from: " + str(addr))
    send_tweets(c)
