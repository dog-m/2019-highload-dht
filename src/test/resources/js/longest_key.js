function onNode() {
    var iter = dao.iteratorFromBeginning();
    var maxLength = -1;
    var record, key, keyLen;

    while (iter.hasNext()) {
        record = iter.getNext();
        key = record.getKey();

        keyLen = key.remaining();
        if (keyLen > maxLength) {
            maxLength = keyLen;
        }
    }

    return maxLength;
}

function onReducer(results) {
    let max = -1;
    for (var value in results) {
        if (value > max) {
            max = value;
        }
    }

    return max;
}

print(dao);
