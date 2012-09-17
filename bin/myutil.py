from HTMLParser import HTMLParser
import time

# Tools for stripping html
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ' '.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def getLinuxTimeStamp(x):
    return int(time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')))


def myparser(html):
    st = ''
    cnt = 0
    for i in html:
        if i == '<':
            cnt += 1
        elif i == '>':
            cnt -= 1
        elif cnt == 0:
            if i == '\n' or i == '\r':
                st += ' '
            else:
                st += i
    return st.replace('\\r',' ').replace('\\n',' ')


def test_myparser():
    for line in file('err'):
        print myparser(line.strip())

if __name__=='__main__':
    test_myparser()
