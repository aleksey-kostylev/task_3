import re

class NameProcessing:
    
    """Класс для обработки названий (имен) торговых точек"""
    
    def __init__(self, input_string):
        self.input_string = input_string # Исходная строка
        self.modified_string = re.sub(r'[^А-Яа-я0-9-]',' ', input_string) # Убираем ненужные символы
        self.clean_left = re.sub(r'^[ ]*(ЗАО|ООО|ОАО|ИП)[ ]+','', self.modified_string) # Убираем folo в начале строки
        self.clean_right = re.sub(r'[ ]+(ЗАО|ООО|ОАО|ИП).*','', self.clean_left) # Убираем folo в конце строки
        self.clean_double_backspace = re.sub(r'\s\s+', ' ', self.clean_right).lower() # Убираем двойные пробелы
        self.cleaned_string = re.sub(r'[ ]$', '', self.clean_double_backspace) # Убираем двойные пробелы, получаем очищенную строку для поиска