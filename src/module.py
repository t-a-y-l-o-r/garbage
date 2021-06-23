import ray
import requests

#   ========================================================================
#                       Table of Contents
#   ========================================================================
#  1. globals
#  n. Main <- always last
#
#
#   ========================================================================
#                       Description
#   ========================================================================
# This module contains a mock for running network calls in parrallel.
# In particular this is to demonstrate and explore how to use the `ray`
# library.
#
# docs: https://docs.ray.io/en/master/index.html
#
#
#

URL = "https://google.com"

@ray.remote
def get():
    return requests.get("https://google.com")

@ray.remote
class Getter():
    def __init__(self, url):
        self.url = url

    def call(self):
        return requests.get("https://google.com")

if __name__ == "__main__":
    ray.init()
    getter = Getter.remote(URL)
    futures = getter.call.remote()
    print(ray.get(futures))
