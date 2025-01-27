import os

def get_latest_file_path(directory_path):

    try:

        if not os.path.isdir(directory_path):
            print("The path is not a directory")
            return None

        excel_files = [
            file for file in os.listdir(directory_path)
            if file.endswith(('.xlsx', '.xls'))
        ]

        if not excel_files:
            print('No excel files found')
            return None

        latest_file = max(excel_files, key=lambda x: os.path.getctime(os.path.join(directory_path, x)))

        latest_file_path = os.path.join(directory_path, latest_file)

        return latest_file_path
    except Exception as e:

        print(f'Exception: {e}')
        return None


def get_subfolder_paths(directory_path):

    try:
        if not os.path.isdir(directory_path):
            print("The path is not a directory")
            return None

        subfolders = [
            os.path.join(directory_path,folder)
            for folder in os.listdir(directory_path)
            if os.path.isdir(os.path.join(directory_path,folder))
        ]

        return subfolders
    except Exception as e:
        print(f'Exception: {e}')
        return None