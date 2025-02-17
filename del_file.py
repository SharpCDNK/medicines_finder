import os


def delete_files_by_name(base_path, target_filename):
    """
    Удаляет файлы с указанным именем из всех папок в указанной директории.

    :param base_path: Путь к папке, содержащей папки конкурентов.
    :param target_filename: Название файла, который нужно удалить.
    """
    # Проверяем, существует ли базовая папка
    if not os.path.exists(base_path):
        print(f"Путь {base_path} не существует.")
        return

    # Проходим по всем вложенным папкам
    for competitor_folder in os.listdir(base_path):
        competitor_path = os.path.join(base_path, competitor_folder)

        # Проверяем, что это папка
        if os.path.isdir(competitor_path):
            target_file_path = os.path.join(competitor_path, target_filename)

            # Если файл существует, удаляем его
            if os.path.exists(target_file_path):
                try:
                    os.remove(target_file_path)
                    print(f"Файл {target_filename} удалён из {competitor_path}.")
                except Exception as e:
                    print(f"Ошибка при удалении файла {target_filename} в {competitor_path}: {e}")
            else:
                print(f"Файл {target_filename} не найден в {competitor_path}.")
        else:
            print(f"{competitor_folder} — это не папка, пропускаем.")


# Базовая папка
base_path = "Datasets/competitors"

# Запрашиваем у пользователя название файла для удаления
target_filename = input("Введите название файла, который нужно удалить: ")

# Вызываем функцию удаления
delete_files_by_name(base_path, target_filename)