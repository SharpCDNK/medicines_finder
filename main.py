import os
from schedule import *  # Import functions from schedule.py

def main():
    global cur_apteka, cur_apteka_link

    cur_apteka = input("Введите название аптеки: ")
    cur_apteka_link = input("Введите ссылку на аптеку: ")
    job(cur_apteka, cur_apteka_link)

if __name__ == '__main__':
    main()