import configparser
import os



class configManager():  
    def __init__(self):
        self.config = None


    def readConfig(self):
        self.config = configparser.ConfigParser()
        cwd = os.path.dirname(os.path.abspath(__file__))
        self.config.read(cwd + "/config.ini")
        return self.config





def main():
    c=configManager()
    c.readConfig()
   

if __name__ == "__main__":
    main()
