package fr.coding.csvreader;

public class InvalidMovie implements Movie{
    private final String tconst;

    public InvalidMovie(String tconst) {
        this.tconst = tconst;
    }

    @Override
    public String getTconst() {
        return tconst;
    }
}
