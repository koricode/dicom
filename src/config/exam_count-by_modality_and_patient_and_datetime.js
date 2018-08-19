var map = function (doc) {
    if (("t0008002A" in doc || ("t00080022" in doc && "t00080032" in doc)) && "t00080060" in doc && "t00100020" in doc) {
        var patient = doc.t00100020,
            modality = doc.t00080060,
            datetime = "t0008002A" in doc ? doc.t0008002A : doc.t00080022 + doc.t00080032;

        var yr = Number(datetime.substr(0, 4)),
            mo = Number(datetime.substr(4, 2)),
            da = Number(datetime.substr(6, 2)),
            hr = Number(datetime.substr(8, 2)),
            mn = Number(datetime.substr(10, 2)),
            sc = Number(datetime.substr(12, 2));

        emit([modality, patient, yr, mo, da, hr, mn, sc], 1);
    }
};

var reduce = function (keys, values, rereduce) {
    return sum(values);
};