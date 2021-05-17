def file_read(fname):
    content_array = []
    with open(fname) as f:
        # Content_list is the list that contains the read lines.
        for line in f:
            content_array.append(line.strip())
        print(content_array)


file_read('/Users/isaacdelgado/Documents/HW/stopwords')
