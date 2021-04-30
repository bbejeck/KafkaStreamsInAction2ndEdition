package bbejeck.chapter_3.model;

import java.util.Objects;

/**
 * Simple model object for use with {@link bbejeck.chapter_3.AvroReflectionProduceConsumeExample}
 * Also used in the JsonSerializerDeserializerTest.
 */
public class User { 
    private String name;
    private int favoriteNumber;
    private String favoriteColor;

    public User() {
    }

    public User(String name, int favoriteNumber, String favoriteColor) {
        this.name = name;
        this.favoriteNumber = favoriteNumber;
        this.favoriteColor = favoriteColor;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof User)) return false;
        User user = (User) o;
        return favoriteNumber == user.favoriteNumber && Objects.equals(name, user.name) && Objects.equals(favoriteColor, user.favoriteColor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, favoriteNumber, favoriteColor);
    }
}
