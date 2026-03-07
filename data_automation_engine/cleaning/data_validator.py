class DataValidator:

    REQUIRED_FIELDS = ["text", "author", "tags"]

    @classmethod
    def validate(cls, data):

        valid_data = []

        for item in data:

            if all(field in item for field in cls.REQUIRED_FIELDS):
                valid_data.append(item)

        return valid_data