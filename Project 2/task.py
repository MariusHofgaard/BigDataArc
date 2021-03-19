
import re
def eliminate_punctuation_symbols(string):
    """
    Input: string
    Method: Removes all characters that are not: word characters, space characters or "DOT" aka "."
    """
    new_string = re.sub(r'[^\w\s\.]','',string)

    return new_string


if __name__ == "__main__":
    word_1 = """
    def eliminate_punctuation_symbols(string):
    Input: string
            Method: Remov##"#es all characters that are not: word characters, s#("#YI¤pace characters or.
    unwanted_symb/##"ols =
    new_string = Regex.Replace(string, @"[^\w\s\.], );
    """

    print(eliminate_punctuation_symbols(word_1))
