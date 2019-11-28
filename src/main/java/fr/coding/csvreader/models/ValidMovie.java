package fr.coding.csvreader.models;

import io.vavr.collection.List;

import java.util.Arrays;
import java.util.Objects;

public class ValidMovie implements Movie {
    private static final String COMMA_DELIMITER = ",";
    private static final String NULL_VALUE = "\\N";
    private final String tconst;
    private final String titleType;
    private final String primaryTitle;
    private final String originalTitle;
    private final Boolean isAdult;
    private final Integer startYear;
    private final Integer endYear;
    private final Integer runtimeMinutes;
    private final List<String> genders;

    private ValidMovie(String tconst, String titleType, String primaryTitle, String originalTitle, Boolean isAdult, Integer startYear, Integer endYear, Integer runtimeMinutes, List<String> genders) {
        this.tconst = tconst;
        this.titleType = titleType;
        this.primaryTitle = primaryTitle;
        this.originalTitle = originalTitle;
        this.isAdult = isAdult;
        this.startYear = startYear;
        this.endYear = endYear;
        this.runtimeMinutes = runtimeMinutes;
        this.genders = genders;
    }

    public static ValidMovie create(List<String> values) {
        return ValidMovie.create(
                values.get(0),
                values.get(1),
                values.get(2),
                values.get(3),
                values.get(4),
                values.get(5),
                values.get(6),
                values.get(7),
                values.get(8)
        );
    }

    private static ValidMovie create(String tconst, String titleType, String primaryTitle, String originalTitle, String isAdult, String startYear, String endYear, String runtimeMinutes, String genders) {
        return new ValidMovie(
                NULL_VALUE.equals(tconst) ? null : tconst,
                NULL_VALUE.equals(titleType) ? null : titleType,
                NULL_VALUE.equals(primaryTitle) ? null : primaryTitle,
                NULL_VALUE.equals(originalTitle) ? null : originalTitle,
                NULL_VALUE.equals(isAdult) ? null : Boolean.valueOf(isAdult),
                NULL_VALUE.equals(startYear) ? null : Integer.valueOf(startYear),
                NULL_VALUE.equals(endYear) ? null : Integer.valueOf(endYear),
                NULL_VALUE.equals(runtimeMinutes) ? null : Integer.valueOf(runtimeMinutes),
                NULL_VALUE.equals(genders) ? List.empty() : List.ofAll(Arrays.asList(genders.split(COMMA_DELIMITER)))
        );
    }

    @Override
    public String getTconst() {
        return tconst;
    }

    public String getTitleType() {
        return titleType;
    }

    public String getPrimaryTitle() {
        return primaryTitle;
    }

    public String getOriginalTitle() {
        return originalTitle;
    }

    public Boolean getAdult() {
        return isAdult;
    }

    public Integer getStartYear() {
        return startYear;
    }

    public Integer getEndYear() {
        return endYear;
    }

    public Integer getRuntimeMinutes() {
        return runtimeMinutes;
    }

    public List<String> getGenders() {
        return genders;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidMovie that = (ValidMovie) o;
        return Objects.equals(tconst, that.tconst) &&
                Objects.equals(titleType, that.titleType) &&
                Objects.equals(primaryTitle, that.primaryTitle) &&
                Objects.equals(originalTitle, that.originalTitle) &&
                Objects.equals(isAdult, that.isAdult) &&
                Objects.equals(startYear, that.startYear) &&
                Objects.equals(endYear, that.endYear) &&
                Objects.equals(runtimeMinutes, that.runtimeMinutes) &&
                Objects.equals(genders, that.genders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genders);
    }

    public boolean matchGender(String gender) {
        return gender != null && genders.contains(gender);
    }
}