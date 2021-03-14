#include "scidbase.h"
#include "searchpos.h"
#include "timer.h"
#include <iostream>

void bench1(const char* dbType, const char* filename, gamenumT gamenum) {
	unsigned long long res = 0;
	scidBaseT base;
	Timer timer;
	base.open(dbType, FMODE_Both, filename);
	std::cout << timer.MilliSecs() << ";open " << filename << " ended;";
	timer.Reset();
	Game game;
	base.getGame(*base.getIndexEntry(gamenum), game);
	game.MoveToStart();
	auto filter = base.getFilter("dbfilter");
	while (game.MoveForward() == OK) {
		SearchPos search(*game.currentPos());
		search.setFilter(base, filter, {});
		res += filter.mainSize();
	}
	std::cout << timer.MilliSecs() << ";search ended;";
	std::cout << res << std::endl;
}

void bench2(const char* dbType, const char* filename, gamenumT gamenum) {
	unsigned long long res = 0;
	scidBaseT base;
	Timer timer;
	base.open(dbType, FMODE_Both, filename);
	std::cout << timer.MilliSecs() << ";open " << filename << " ended;";
	timer.Reset();
	Game game;
	base.getGame(*base.getIndexEntry(gamenum), game);
	game.MoveToStart();
	const auto n = base.numGames();
	while (game.MoveForward() == OK) {
		SearchPos search(*game.currentPos());
		for (uint gnum = 0; gnum < n; gnum++) {
			if (search.match(base, gnum).first > 0)
				++res;
		}
	}
	std::cout << timer.MilliSecs() << ";search ended;";
	std::cout << res << std::endl;
}

int main() {
	bench1("PGN", "bench.pgn", 16);
	bench1("PGN", "bench_stripped.pgn", 16);
	bench1("SCID4", "bench", 84254);
	bench1("PGN", "bench_sorted.pgn", 84254);
}