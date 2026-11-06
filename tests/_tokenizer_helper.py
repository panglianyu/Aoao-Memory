"""Small offline tokenizer boundary shared by synthetic adapter tests."""


class CharacterTokenizer:
    is_fast = True

    def __call__(self, text, **kwargs):
        assert kwargs["truncation"] is False
        offsets = [(0, 0)]
        for index, char in enumerate(text):
            if not char.isspace():
                offsets.extend([(index, index + 1)] * (3 if char == "龘" else 1))
        offsets.append((0, 0))
        return {"input_ids": list(range(len(offsets))), "offset_mapping": offsets}
