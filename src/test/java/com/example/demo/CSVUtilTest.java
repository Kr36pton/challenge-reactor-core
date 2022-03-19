package com.example.demo;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

public class CSVUtilTest {

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }
    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));
        assert listFilter.size() == 322;
    }
    @Test
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .filter(player -> !player.club.isEmpty())
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .sort((player1, player2) -> player1.getName().compareToIgnoreCase(player2.getName()))
                .collectMultimap(Player::getClub);

        listFilter.subscribe((playerNationalities)->
                playerNationalities.entrySet().
                        forEach((club)->
                                club.getValue()
                                        .forEach((players)-> System.out.println(
                                                club.getKey() + ":  " + players.getName())
                                )
                        )
        );
        assert listFilter.block().get("Sporting Lokeren").size() == 28;
        assert listFilter.block().get("SD Huesca").size() == 30;
        assert listFilter.block().get("Bradford City").size() == 28;
    }

    @Test
    public void reactive_ranking_nationalities()
    {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> ranking = listFlux.collectMultimap(Player::getNational);
        ranking.subscribe(stringCollectionMap -> stringCollectionMap.entrySet()
                .forEach(stringCollectionEntry ->
                {
                    System.out.println(stringCollectionEntry.getKey() + ":  " +
                            stringCollectionEntry.getValue().size());
                }
                )
        );
        assert ranking.block().get("Uruguay").size() == 149;
        assert ranking.block().get("Colombia").size() == 618;
        assert ranking.block().get("Canada").size() == 64;
    }

}