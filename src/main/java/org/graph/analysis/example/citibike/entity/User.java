package org.graph.analysis.example.citibike.entity;

import org.graph.analysis.entity.Vertex;

import java.util.UUID;

public class User extends Vertex {
    public String userType;
    public String birthYear;
    public Gender gender;

    public User(String userType, String birthYear, String gender) {
        super(
                UUID.randomUUID().toString().replace("-", "").toUpperCase(),
                VertexLabel.User.getLabel()
        );
        this.userType = userType;
        this.birthYear = birthYear;
        this.gender = Gender.fromId(Integer.parseInt(gender));
    }

    @Override
    public String toString() {
        String properties = String.format(
                "\"properties\":" +
                        "{" +
                        "\"gender\":\"%s\"," +
                        "\"userType\":\"%s\"," +
                        "\"birthYear\":\"%s\"" +
                        "}",
                gender.getCode(), userType, birthYear);
        return String.format("%s,%s}", super.toString().replace("}", ""), properties);
    }
}
