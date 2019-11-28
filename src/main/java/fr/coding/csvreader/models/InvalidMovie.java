package fr.coding.csvreader.models;

import java.util.Objects;

public class InvalidMovie implements Movie{
    private final String tconst;

    public InvalidMovie(String tconst) {
        this.tconst = tconst;
    }

    @Override
    public String getTconst() {
        return tconst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvalidMovie that = (InvalidMovie) o;
        return Objects.equals(tconst, that.tconst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tconst);
    }
}
