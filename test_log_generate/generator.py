import sys
import random
import time
import json
import os

urls = [
    "https://blog.griddynamics.com/in-stream-processing-service-blueprint",
    "https://blog.griddynamics.com/index",
    "https://griddynamics.com/index",
    "https://youtube.com/index",
    "https://google.com/index",
    "https://facebook.com/index",
    "https://apple.com/index"
]
urls_max = len(urls) - 1

# there will be 4 or less bots
bots = [
    "192.168.1.100",
    "192.168.2.1",
    "172.10.1.100",
    "10.10.10.10",
]
bots_max = len(bots) - 1

# event types
eventType = {"click": "click", "watch": "watch", "incorrect": "blablabla"}


def main():
    if len(sys.argv) != 4:
        print("using: python " + sys.argv[0] + " {log-folder} {files} {records}")
        sys.exit(-1)

    for i in range(0, int(sys.argv[2]), 1):
        filename = "requests_{:0>6}.log".format(i)
        print("creating file " + sys.argv[1] + "/" + filename)

        log = open(sys.argv[1] + "/" + filename, "w+")
        good_watchers = []
        for r in range(0, int(sys.argv[3]), 1):
            r = random.random()
            if r < 0.8:
                # add new watch request
                if r < 0.4 and len(good_watchers) > 0:
                    ip = good_watchers.pop()
                    log.write(generate_watch(ip))
                else:
                    ip = random_ip()
                    good_watchers.append(ip)
                    log.write(generate_click(ip))
            else:
                ip = bots[random.randint(0, bots_max)]
                if random.random > 0.8:
                    log.write(generate_click(ip))
                else:
                    log.write(generate_watch(ip))

            log.write("\n")

        log.close()
    return


def random_ip():
    addr = bytearray(os.urandom(4))
    return "{0}.{1}.{2}.{3}".format(addr[0], addr[1], addr[2], addr[3])


def generate_click(ip):
    return generate(ip, eventType.get("click"))


def generate_watch(ip):
    return generate(ip, eventType.get("watch"))


def generate(ip, action):
    return json.dumps({
        "type": action,
        "ip": ip,
        "unix_time": int(round(time.time() * 1000)),
        "url": urls[random.randint(0, urls_max)]
    })


if __name__ == '__main__':
    main()
