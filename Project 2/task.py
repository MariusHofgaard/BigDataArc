def remove_short_words(line):
    """

    :param line: inp
    :return:
    """
    try:
        for index,item in enumerate(line[1]): # enumerate the list for popping
            if len(item) < 3:
                line[1].pop(index)
    except:
        pass

    return line


if __name__ == "__main__":
    line = [a]