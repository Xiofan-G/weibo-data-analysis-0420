package org.graph.analysis.example.citibike.entity;


public enum Gender {
    /**
     * User gender
     */
    Unknown(0, "Unknown"),
    Male(1, "Male"),
    Female(2, "Female");

    private Integer id;
    private String code;

    Gender(int id, String code) {
        this.id = id;
        this.code = code;

    }

    public static Gender fromId(Integer id) {
        for (Gender gender : Gender.values()) {
            if (gender.getId().equals(id)) {
                return gender;
            }
        }
        return Unknown;
    }

    public Integer getId() {
        return id;
    }

    public String getCode() {
        return code;
    }

}

