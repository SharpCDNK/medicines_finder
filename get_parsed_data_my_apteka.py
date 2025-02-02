from parser import *

my_apteka = input('Моя аптека: ')
my_apteka_link = input('Ссылка:')

save_directory = os.path.join(os.getcwd(), 'Datasets', 'our_pharmacies', my_apteka)
os.makedirs(save_directory, exist_ok=True)
get_parser_data(my_apteka_link,save_directory)

