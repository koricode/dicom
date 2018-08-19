var map = function (doc) {
    if (("t0008002A" in doc || ("t00080022" in doc && "t00080032" in doc)) && "t00080060" in doc) {
        var value = {
            t00080018: doc.t00080018,
            t00100020: doc.t00100020
        }, isEmit = false;

        ["t00180060", "t00181150", "t00181151", "t00189321", "t00189345", "t00181405"].forEach(function (tag) {
            if (tag in doc) {
                value[tag] = doc[tag];
                isEmit = true;
            }
        });

        if (isEmit) {
            var modality = doc.t00080060,
                datetime = "t0008002A" in doc ? doc.t0008002A : doc.t00080022 + doc.t00080032;

            var yr = Number(datetime.substr(0, 4)),
                mo = Number(datetime.substr(4, 2)),
                da = Number(datetime.substr(6, 2)),
                hr = Number(datetime.substr(8, 2)),
                mn = Number(datetime.substr(10, 2)),
                sc = Number(datetime.substr(12, 2));

            emit([modality, yr, mo, da, hr, mn, sc], value);
        }
    }
};