package com.datastax.astra.beam;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

@Table(name = LanguageCode.TABLE_NAME)
public class CharacterRM implements Serializable {

    public static final String TABLE_NAME      = "characters";
    public static final String COLUMN_ORIGIN = "origin";
    public static final String COLUMN_ID     = "id";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_ALIVE = "alive";
    public static final String COLUMN_SPECIES = "species";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_GENDER = "gender";

    public static final String COLUMN_LOCATION = "location";
    public static final String COLUMN_IMAGE = "image";
    public static final String COLUMN_EPISODES = "episodes";
    public static final String COLUMN_URL = "url";
    public static final String COLUMN_CREATED = "created";

    @PartitionKey
    @Column(name = COLUMN_ORIGIN)
    private String origin;

    @ClusteringColumn
    @Column(name = COLUMN_ID)
    private int id;

    @Column(name = COLUMN_NAME)
    private String name;

    @Column(name = COLUMN_ALIVE)
    private boolean alive;

    @Column(name = COLUMN_SPECIES)
    private String species;

    @Column(name = COLUMN_TYPE)
    private String type;

    @Column(name = COLUMN_GENDER)
    private String gender;

    @Column(name = COLUMN_LOCATION)
    private String location;

    @Column(name = COLUMN_IMAGE)
    private String image;

    @Column(name = COLUMN_EPISODES)
    private int episodes;

    @Column(name = COLUMN_URL)
    private String url;

    @Column(name = COLUMN_CREATED)
    private String created;

    public CharacterRM() {
    }

    public CharacterRM(int id,
    String name,
    boolean alive,
    String species,
     String type,
    String gender,
    String origin,
    String location,
    String url,
    String image,
    String created,
    int episodes) {
this.origin = origin;
this.id = id;
this.name = name;
this.alive = alive;
this.species = species;
this.type = type;
this.gender = gender;
this.location = location;
this.image = image;
this.episodes = episodes;
this.url = url;
this.created = created;
}

public static CharacterRM fromCsvRow(String csvRow) {
String[] chunks = csvRow.split(",");
return new CharacterRM(Integer.parseInt(chunks[0]),chunks[1], chunks[2].equals("Alive"), chunks[3],  chunks[4], chunks[5], chunks[6], chunks[7], chunks[8], chunks[9], chunks[10], Integer.parseInt(chunks[11]));
}

public String toCsvRow() {
return origin + "," + id + "," + name + "," + alive + "," + species + "," + type + "," + gender + "," + location + "," + image + "," + episodes + "," + url + "," + created;
}

public static CharacterRM fromCsv(String csvRow) {
String[] chunks = csvRow.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
return new CharacterRM(Integer.parseInt(chunks[0]),chunks[1], chunks[2].equals("Alive"), chunks[3],  chunks[4], chunks[5], chunks[6], chunks[7], chunks[8], chunks[9], chunks[10], Integer.parseInt(chunks[11]));
}

    public static String cqlCreateTable() {
        return SchemaBuilder.createTable(TABLE_NAME)
                .addPartitionKey(COLUMN_ORIGIN, DataType.text())
                .addClusteringColumn(COLUMN_ID, DataType.cint())
                .addColumn(COLUMN_NAME, DataType.text())
                .addColumn(COLUMN_ALIVE, DataType.cboolean())
                .addColumn(COLUMN_SPECIES, DataType.text())
                .addColumn(COLUMN_TYPE, DataType.text())
                .addColumn(COLUMN_GENDER, DataType.text())
                .addColumn(COLUMN_LOCATION, DataType.text())
                .addColumn(COLUMN_IMAGE, DataType.text())
                .addColumn(COLUMN_EPISODES, DataType.cint())
                .addColumn(COLUMN_URL, DataType.text())
                .addColumn(COLUMN_CREATED, DataType.text())
                .ifNotExists().toString();
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public int getEpisodes() {
        return episodes;
    }

    public void setEpisodes(int episodes) {
        this.episodes = episodes;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }
}
