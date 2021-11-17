class MissingCategoryTranslation(Exception):
    """Raised when the input value is too small"""
    def __init__(self, category):
        message = f'Category "{category}" not in Translation Dict'
        super().__init__(message)
