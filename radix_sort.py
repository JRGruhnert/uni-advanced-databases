def radix_sort_by_key(array, sort_key):
    # Determine the data type of the sort_key (string or integer)
    #is_sort_key_string = isinstance(data_list[0][sort_key], str)

    #if is_sort_key_string:
        # If the sort_key is a string, convert it to a list of ASCII values
    #    for i in range(len(data_list)):
    #        data_list[i][sort_key] = [ord(char) for char in data_list[i][sort_key]]

    # Find the maximum length of the sort_key value among all dictionaries
    max_len = max(len(array[i][sort_key]) for i in range(len(array)))

    # Perform the radix sort
    for digit_index in range(max_len - 1, -1, -1):
        buckets = [[] for _ in range(256)]

        # Distribute the dictionaries into buckets based on the current digit
        for data in array:
            key_value = data[sort_key]
            current_digit = key_value[digit_index] if digit_index < len(key_value) else 0
            buckets[current_digit].append(data)

        # Reconstruct the data_list by merging the buckets
        sorted_data_list = []
        for bucket in buckets:
            sorted_data_list.extend(bucket)

        # Update data_list reference to point to the sorted_data_list
        array = sorted_data_list

    #if is_sort_key_string:
    #    # If the sort_key was originally a string, convert it back to a string
    #    for i in range(len(data_list)):
    #        data_list[i][sort_key] = ''.join(chr(char) for char in data_list[i][sort_key])

    return array