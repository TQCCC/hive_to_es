import codecs


class FileUtil:
    @staticmethod
    def get_file_content(path):
        """
        读文件
        :param path:
        :return:
        """
        f = open(path, 'r')
        return f.read()

    @staticmethod
    def get_file_content_by_encoding(path, encoding):
        """
        定义编码格式读文件
        :param path:
        :param encoding:
        :return:
        """
        file = codecs.open(filename=path, mode='r', encoding=encoding, errors='ignore')
        return file.read()
