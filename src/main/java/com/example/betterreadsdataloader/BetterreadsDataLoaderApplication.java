package com.example.betterreadsdataloader;

import com.example.betterreadsdataloader.author.AuthorRepository;
import com.example.betterreadsdataloader.book.Book;
import com.example.betterreadsdataloader.book.BookRepository;
import com.example.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    @Value("${datastax.astra.secure-connect-bundle}")
    private String connect;

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;


    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

//    @PostConstruct
//    public void start() {
//        System.out.println("start started");
//
//        var path = Paths.get(authorDumpLocation);
//        System.out.println(Paths.get(connect));
//        System.out.println(path);
//        try (Stream<String> lines = Files.lines(path)) {
//            lines.forEach(line -> {
//                var jsonString = line.substring(line.indexOf("{"));
//                try {
//                    var jsonObject = new JSONObject(jsonString);
//
//                    var author = new Author();
//                    author.setName(jsonObject.optString("name"));
//                    author.setPersonalName(jsonObject.optString("personal_name"));
//                    author.setId(jsonObject.optString("key").replace("/authors/", ""));
//
//                    System.out.println(author);
//                    authorRepository.save(author);
//                } catch (JSONException e) {
//                    e.printStackTrace();
//                }
//            });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    @PostConstruct
    public void initWorks() {
        System.out.println("initWorks started");

        var path = Paths.get(worksDumpLocation);
        var dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        System.out.println(Paths.get(connect));
        System.out.println(path);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                var jsonString = line.substring(line.indexOf("{"));
                try {
                    var jsonObject = new JSONObject(jsonString);

                    var book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/", ""));
                    book.setName(jsonObject.optString("title"));
                    var descObj = jsonObject.optJSONObject("description");
                    if (descObj != null) {
                        book.setDescription(descObj.optString("value"));
                    }
                    var publishedObj = jsonObject.optJSONObject("created");
                    if (publishedObj != null) {
                        var dateStr = publishedObj.getString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, dateTimeFormatter));
                    }

                    var coversJSONArr = jsonObject.optJSONArray("covers");
                    if (coversJSONArr != null) {
                        var coverIds = new ArrayList<String>();
                        for (int i = 0; i < coversJSONArr.length(); i++) {
                            coverIds.add(coversJSONArr.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    var authorsJSONArr = jsonObject.optJSONArray("authors");
                    if (coversJSONArr != null) {
                        var authorIds = new ArrayList<String>();
                        for (int i = 0; i < authorsJSONArr.length(); i++) {
                            var authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author").getString("key")
                                    .replace("/authors/", "");
                            authorIds.add(authorId);
                        }
                        book.setAuthorIds(authorIds);
                        var authorNames = authorIds.stream().map(id -> authorRepository.findById(id)).map(optionalAuthor -> {
                            if (!optionalAuthor.isPresent()) return "Unknown Author";
                            return optionalAuthor.get().getName();
                        }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }
                    System.out.println(book);
                    bookRepository.save(book);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
    }
}
