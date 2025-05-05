### tactics.tcl: part of Scid.
### Copyright (C) 2007  Pascal Georges
### Copyright (C) 2015 Fulvio Benini
###
######################################################################
### Solve tactics (mate in n moves for example)
# use Site token in pgn notation to store progress
#

namespace eval tactics {

    set infoEngineLabel ""
    set solved "problem solved"
    set failed "problem failed"
    set prevScore 0
    set prevPly 0
    set prevLine ""
    set nextEngineMove ""
    set matePending 0
    set cancelScoreReset 0
    set showSolution 0
    set prevFen ""
    set engineName ""
    set analysisTime 2000
    # Don't try to find the exact best move but to win a won game (that is a mate in 5 is ok even if there was a pending mate in 2)
    set winWonGame 0

    proc getBaseTypeFromFile { fname } {
        set dbType "SCID5"
        set ext [string tolower [file extension "$fname"] ]
        if {$ext == ".si4" } {
            set dbType "SCID4"
        }
        return $dbType
    }
    ################################################################################
    # Tacticts training
    ################################################################################
    proc configBases {win} {
        $win.d.search configure -state disabled
        $win.fbutton.ok configure -state disabled -command {}
        $win.fbutton.reset configure -state disabled -command {}
        $win.fbutton.cancel configure -text [tr stop] -command {progressBarCancel}
        $win.fbutton.help configure -state disabled
        $win.s.bases delete [$win.s.bases children {}]

        set prevBase [sc_base current]
        set valid {}
        set fileList [lsort -dictionary [ glob -nocomplain -directory $::scidBasesDir *.{si4,si5} ] ]
        set progress 0.0
        set progressIncr 1.0
        catch { set progressIncr [expr {602.0 / [llength $fileList]}] }
        busyCursor .
        foreach fname $fileList {
            set name [file rootname [file nativename $fname]]
            set fname [file nativename $fname]
            set baseId [sc_base slot $name]
            if {$baseId == 0} {
                progressBarSet $win.dummy 100 10
                if { [catch { sc_base open [getBaseTypeFromFile $fname] $name } baseId] } {
                    if {$::errorCode == $::ERROR::UserCancel} { break }
                    ERROR::MessageBox
                    continue
                }
                set wasOpened 0
            } else  {
                set wasOpened 1
            }

            set filter [sc_filter new $baseId]
            progressBarSet $win.dummy 100 10
            set err [catch {
                sc_filter search $baseId $filter header -filter RESET -flag S -flag| T
                set nTactics [sc_filter count $baseId $filter]
                sc_filter search $baseId $filter header -filter AND -site "\"$::tactics::solved\""
                set solvedCount [sc_filter count $baseId $filter]

                set desc {}
                foreach {tagname tagvalue} [sc_base extra $baseId] {
                    if {$tagname eq "description"} {
                        set desc $tagvalue
                        break
                    }
                }
                set line [list [file tail $fname] $desc $solvedCount $nTactics]

                set pos "end"
                if {[getBaseType $baseId] == 15} {
                    if {![info exists nTactBases]} {
                        set nTactBases -1
                        set valid $fname
                    }
                    set pos [incr nTactBases]
                }
                $win.s.bases insert {} $pos -id $fname -values $line
                $win.s.bases see $fname
                if {$nTactics == 0} {
                    $win.s.bases item $fname -tag empty
                } else {
                    if {$valid == ""} { set valid $fname }
                }

                set progress [expr {$progress + $progressIncr +1}]
                $win.pbar coords bar 0 0 [expr {int($progress)}] 12
            }]
            sc_filter release $baseId $filter
            if {$wasOpened == 0} {
                sc_base close $baseId
            }

            if {$err} {
                if {$::errorCode == $::ERROR::UserCancel} { break }
                ERROR::MessageBox
                continue
            }
        }
        unbusyCursor .
        sc_base switch $prevBase
        $win.pbar coords bar 0 0 602 12
        $win.fbutton.help configure -state normal
        grid $win.fbutton.reset
        set eager [$win.s.bases selection]
        if {$eager != ""} {
            set valid $eager
            $win.s.bases selection set {}
        }
        bind $win.s.bases <<TreeviewSelect>> "::tactics::configValidBase $win"
        $win.s.bases selection set [list $valid]
        $win.s.bases see $valid
        $win.d.search configure -state normal
    }

    proc configValidDir {win} {
        bind $win.s.bases <<TreeviewSelect>> {}
        $win.fbutton.reset configure -state disabled -command {}
        $win.fbutton.ok configure -state disabled -command {}
        $win.d.search configure -text $::tr(Search)
        $win.pbar coords bar 0 0 0 0
        $win.fbutton.cancel configure -text [tr Cancel] -command "focus .; destroy $win"
        if {[file isdirectory $::scidBasesDir]} {
            $win.d.basedir configure -style {}
            $win.d.search configure -state normal \
                -command "::tactics::configBases $win"
            after idle "after 1 ::tactics::configBases $win"
        } else {
            $win.d.basedir configure -style Error.TEntry
            $win.d.search configure -state disabled -command {}
        }
    }

    proc configValidBase {win} {
        set fname [$win.s.bases selection]
        $win.fbutton.cancel configure -text [tr Cancel] -command "focus .; destroy $win"
        if {$fname != "" && [$win.s.bases item {*}$fname -tags] != "empty"} {
#set $w.e.movetime.value get [ expr $::tactics::analysisTime / 1000];
            $win.fbutton.ok configure -state normal \
                -command "destroy $win; ::tactics::createWin $fname"
            $win.fbutton.reset configure -state normal \
                -command "::tactics::resetScores $fname; ::tactics::configValidDir $win"
        } else {
            $win.fbutton.ok configure -state disabled -command {}
            $win.fbutton.reset configure -state disabled -command {}
        }
    }

    proc config {} {
        # check if tactics window is already opened. If so, abort serial.
        set w .tacticsWin
        if {[winfo exists $w]} {
            destroy $w
        }

        set w ".configTactics"
        if {[winfo exists $w]} {
            focus $w
            return
        }
        win::createDialog $w
        wm title $w $::tr(ConfigureTactics)
        wm resizable $w 0 0

        #dummy progressbar
        canvas $w.dummy
        $w.dummy create rectangle 0 0 0 0

        #Engine selection
        ttk::labelframe $w.e -text "[tr Engine]:"
        ::engineNoWin::createEngineOptionsFrame $w tacticEngine ::tactics::engineName 5 ::tactics::eng_messages
        grid $w.e -sticky ws
        pack $w.tacticEngine -in $w.e -side top -pady 5 -anchor w -padx 4

        ttk::frame $w.e.movetime
        ttk::label $w.e.movetime.l -text "[tr SecondsPerMove]: "
        ttk::spinbox $w.e.movetime.value -width 3 -from 1 -to 120 -increment 1 -validate all -validatecommand { regexp {^[0-9]+$} %P }
        $w.e.movetime.value set [ expr $::tactics::analysisTime / 1000]
        pack $w.e.movetime.l $w.e.movetime.value -side left
        pack $w.e.movetime -side top -anchor w

        #BaseDir selection
        grid [ttk::frame $w.sep2 -height 20] -sticky nwes
        grid [ttk::frame $w.d] -sticky news
        ttk::label $w.d.lbl -font font_Bold -text "[tr ChooseTrainingBase]:"
        grid $w.d.lbl -sticky w -columnspan 3
        grid columnconfigure $w.d 1 -weight 1
        ttk::button $w.d.selectDir -text "..." -command "getTacticsBasesDir $w.d.basedir; ::tactics::configValidDir $w"
        ttk::entry $w.d.basedir -textvariable scidBasesDir  -width 30
        ttk::button $w.d.search -text [tr Search]
        grid $w.d.basedir $w.d.selectDir $w.d.search -sticky w -padx "5 0"


        #Base selection
        grid [ttk::frame $w.s] -sticky news
        ttk::treeview $w.s.bases -columns {0 1 2 3} -show headings -selectmode browse -height 8
        $w.s.bases tag configure empty -foreground #a5a2ac
        $w.s.bases heading 0 -text [tr DatabaseName]
        $w.s.bases heading 1 -text [tr Description]
        $w.s.bases heading 2 -text Solved
        $w.s.bases heading 3 -text [tr Total]
        $w.s.bases column 0 -width 140
        $w.s.bases column 1 -width 300
        $w.s.bases column 2 -width 80 -anchor c
        $w.s.bases column 3 -width 80 -anchor c
        autoscrollframe -bars both $w.s "" $w.s.bases

        canvas $w.pbar -width 600 -height 10 -bg white -relief solid -border 1
        $w.pbar create rectangle 0 0 0 0 -fill blue -outline blue -tags bar
        grid $w.pbar -pady "0 5"


        #Buttons
        grid [ttk::frame $w.fbutton] -sticky news
        grid columnconfigure $w.fbutton 1 -weight 1
        ttk::button $w.fbutton.ok -text [tr Continue]
        ttk::button $w.fbutton.cancel
        ttk::button $w.fbutton.reset -text [tr ResetScores]
        ttk::button $w.fbutton.help -text [tr Help] \
            -command "destroy $w; helpWindow TacticsTrainer"
        grid $w.fbutton.ok -row 0 -column 2 -padx 10
        grid $w.fbutton.cancel -row 0 -column 3
        grid $w.fbutton.reset -row 0 -column 1 -sticky w -padx 10
        grid $w.fbutton.help -row 0 -column 0 -sticky w

        # Set up geometry for middle of screen:
        set x [expr ([winfo screenwidth $w] - 600) / 2]
        set y [expr ([winfo screenheight $w] - 600) / 2]
        wm geometry $w +$x+$y
        grab $w
        configValidDir $w
        focus $w
    }
    ################################################################################
    #
    ################################################################################
    proc createWin { base } {
        global ::tactics::analysisEngine

        set analysisEngine(analyzeMode) 0
        if { [::tactics::loadBase $base] } { return }
        if { ! [::engineNoWin::initEngine tacticEngine $::tactics::engineName \
                    [list ::tactics::eng_messages tacticEngine nop]] } {
            return
        }

        set w .tacticsWin
        if {[winfo exists $w]} { focus $w ; return }

        createToplevel $w .pgnWin
        setTitle $w $::tr(Tactics)
        applyThemeColor_background $w
        # because sometimes the 2 buttons at the bottom are hidden
        wm minsize $w 170 170
        ttk::frame $w.f1
        ttk::label $w.f1.labelInfo -textvariable ::tactics::infoEngineLabel
        pack $w.f1.labelInfo -side top  -fill x

        ttk::frame $w.fclock
        ::gameclock::new "" 1
        ::gameclock::reset 1
        ::gameclock::start 1
        ttk::label $w.fclock.l -text $::tr(Time)
        ttk::label $w.fclock.time -textvariable ::gamePlayers(clockW)
        pack $w.fclock.time $w.fclock.l -side right -fill x

        ttk::frame $w.f2
        ttk::checkbutton $w.f2.cbSolution -text $::tr(ShowSolution) -variable ::tactics::showSolution -command ::tactics::toggleSolution
        ttk::checkbutton $w.f2.cbWinWonGame -text $::tr(WinWonGame) -variable ::tactics::winWonGame
        ttk_text $w.lSolution -style Label -wrap word -relief flat -height 1 -width 40
        pack $w.f2.cbSolution -side left -anchor w
        pack $w.f2.cbWinWonGame -side right -anchor e

        ttk::frame $w.fbuttons
        pack $w.f1 $w.fclock $w.f2 $w.lSolution $w.fbuttons -fill x -padx 10 -pady 2

        setInfoEngine $::tr(LoadingBase)

        ttk::button $w.fbuttons.next -text $::tr(Next) -padding {20 0} -command {
            ::tactics::stopAnalyze
            ::tactics::loadNextGame }
        ttk::button $w.fbuttons.close -textvar ::tr(Abort) -padding {20 0} -command "destroy $w"
        pack $w.fbuttons.next $w.fbuttons.close -side right -fill x -padx { 20 0 }
        bind $w <Destroy> "if {\[string equal $w %W\]} {::tactics::endTraining}"
        bind $w <F1> { helpWindow TacticsTrainer }
        createToplevelFinalize $w

        setInfoEngine "---"
        ::setPlayMode "::tactics::callback"
        ::tactics::loadNextGame
    }

    proc ::tactics::eng_messages {id w msg} {
        global ::tactics::analysisEngine
        lassign $msg msgType msgData
        switch $msgType {
          "InfoConfig" {
              if { ! [winfo exists $w] } { return }
              set msgData [lindex $msgData 2]
              ::engineNoWin::initEngineOptions $id $w $msgData
          }
          "InfoPV" {
              lassign $msgData multipv depth seldepth nodes nps hashfull tbhits time score score_type score_wdl pv
              if { $multipv == 1 } {
                  if { $score_type ne "mate" } {
                      set analysisEngine(score) [expr $score / 100.0]
                      set analysisEngine(mateply) 0
                  } else {
                      set analysisEngine(mateply) $score
                      if { $score > 0 } {
                          set analysisEngine(score) 512.0
                      } else {
                          set analysisEngine(score) -512.0
                      }
                  }
                  set analysisEngine(moves) $pv
              }
          }
          "InfoBestMove" {
              lassign $msgData ::tactics::data(bestmove) ponder ::tactics::data(ponder)
              set ::analysisEngine(move_done) 1
          }
          "InfoDisconnected" {
              lassign $msgData errorMsg
              if {$errorMsg eq ""} { set errorMsg "The connection with the engine terminated unexpectedly." }
              tk_messageBox -icon warning -type ok -parent . -message $errorMsg
              ::tactics::abortGame
          }
        }
    }

    proc callback {cmd args} {
        switch $cmd {
            premove { # TODO: currently we just return true if it is the engine turn.
                return [expr { ! [::tactics::isPlayerTurn] }]
            }
            stop { destroy .tacticsWin }
        }
        return 0
    }

    proc endTraining {} {
        after cancel ::tactics::mainLoop
        after cancel ::tactics::loadNextGame
        ::tactics::stopAnalyze

        #TODO:
        #sc_filter release $::tactics::baseId $::tactics::filter
        sc_filter reset $::tactics::baseId dbfilter full
        unset ::enginewin::engConfig_tacticEngine
        ::engine::close tacticEngine
        ::gameclock::stop 1

        ::setPlayMode ""
        ::board::flipAuto .main.board
        updateStatusBar
        updateTitle
    }
    ################################################################################
    #
    ################################################################################
    proc toggleSolution {} {
        global ::tactics::showSolution ::tactics::analysisEngine
        set w .tacticsWin
        if {$showSolution} {
            set pv [sc_pos coordToSAN [sc_pos fen] $analysisEngine(moves)]
            set labelSolution "$analysisEngine(score) : [::trans $pv]"
            $w.lSolution configure -height [expr int([string length $labelSolution]/50)]
            $w.lSolution delete 1.0 end
            $w.lSolution insert end $labelSolution
        } else  {
            $w.lSolution delete 0.0 end
            $w.lSolution configure -height 1
        }
    }
    ################################################################################
    #
    ################################################################################
    proc resetScores {fname} {
        global ::tactics::cancelScoreReset

        set prevBase [sc_base current]
        set baseId [sc_base slot $fname]
        if {$baseId == 0} {
            if { [catch { sc_base open [getBaseTypeFromFile $fname] $fname } baseId] } {
                ERROR::MessageBox
                continue
            }
            set wasOpened 0
        } else  {
            sc_base switch $baseId
            set curr_game [sc_game number]
            sc_game push
            set wasOpened 1
        }
        set filter [sc_filter new $baseId]
        sc_filter search $baseId $filter header -filter RESET -site "\"$::tactics::solved\""

        #reset site tag for each game
        set numGames [sc_filter count $baseId $filter]
        set cancelScoreReset 0
        progressWindow "Scid" $::tr(ResettingScore) $::tr(Cancel) "set ::tactics::cancelScoreReset 1"
        for {set g 0} {$g < $numGames && $cancelScoreReset == 0} {incr g 100} {
            updateProgressWindow $g $numGames

            foreach {idx line deleted} [sc_base gameslist $baseId $g 100 $filter N+] {
                foreach {n ply} [split $idx "_"] {
                    sc_game load $n
                    sc_game tags set -site ""
                    sc_game save [sc_game number]
                }
            }
        }
        closeProgressWindow
        sc_filter release $baseId $filter
        if { ! $wasOpened } {
            sc_base close $baseId
        } else {
            if {$curr_game == 0} {
                sc_game new
            } else {
                sc_game load $curr_game
            }
            sc_game pop
            ::notify::DatabaseModified $baseId
            ::notify::GameChanged
        }
        sc_base switch $prevBase
    }
    ################################################################################
    #
    ################################################################################
    proc loadNextGame {} {
        global ::tactics::analysisEngine
        ::tactics::resetValues
        setInfoEngine $::tr(LoadingGame)

        set nextTactic 0
        while {![sc_pos isAt end]} {
            sc_move forward
            set cmt [sc_pos getComment]
            sc_var exit;
            if {[regexp {^\*\*\*\*D?[0-9]} $cmt]} {
                set nextTactic 1
                break
            }
        }
        if {$nextTactic == 0} {
            set g [sc_filter next]
            if {$g == 0} {
                tk_messageBox -title "Scid" -icon info -type ok -message $::tr(AllExercisesDone)
                return
            }
            sc_game load $g
            if {[sc_pos fen] == "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"} {
                after idle ::tactics::loadNextGame
                return
            }
        }
        ::notify::GameChanged
        ::board::flipAuto .main.board [expr {[sc_pos side] == "black"}]
        focus .main

        ::gameclock::reset 1
        ::gameclock::start 1

        set ::tactics::prevFen [sc_pos fen]
        ::tactics::startAnalyze
        #needs complement
        set analysisEngine(score) [expr 0.0 - $analysisEngine(score)]
        set analysisEngine(mateply) [expr 0 - $analysisEngine(mateply)]
        ::tactics::mainLoop
    }
    ################################################################################
    #
    ################################################################################
    proc isPlayerTurn {} {
        if { [sc_pos side] == "white" &&  ![::board::isFlipped .main.board] || [sc_pos side] == "black" &&  [::board::isFlipped .main.board] } {
            return 1
        }
        return 0
    }
    ################################################################################
    #
    ################################################################################
    proc exSolved {} {
        ::tactics::stopAnalyze
        ::gameclock::stop 1
        tk_messageBox -title "Scid" -icon info -type ok -message $::tr(MateFound)
        sc_game tags set -site $::tactics::solved
        sc_game save [sc_game number]
        ::tactics::loadNextGame
    }
    ################################################################################
    # Handle the case where position was changed not during normal play but certainly with
    # move back / forward / rewind commands
    ################################################################################
    proc abnormalContinuation {} {
        ::tactics::stopAnalyze
        ::tactics::resetValues
        ::notify::GameChanged
        ::notify::DatabaseChanged
        if { [sc_pos side] == "white" && [::board::isFlipped .main.board] || [sc_pos side] == "black" &&  ![::board::isFlipped .main.board] } {
            ::board::flip .main.board
        }
        set ::tactics::prevFen [sc_pos fen]
        ::tactics::startAnalyze
        ::tactics::mainLoop
    }
    ################################################################################
    # waits for the user to play and check the move played
    ################################################################################
    proc mainLoop {} {
        global ::tactics::prevScore ::tactics::prevLine ::tactics::prevPly ::tactics::analysisEngine ::tactics::nextEngineMove

        after cancel ::tactics::mainLoop

        if {[sc_pos fen] != $::tactics::prevFen && [sc_pos isAt start]} {
            ::tactics::abnormalContinuation
            return
        }

        # is this player's turn (which always plays from bottom of the board) ?
        if { [::tactics::isPlayerTurn] } {
            after 1000  ::tactics::mainLoop
            return
        }

        set ::tactics::prevFen [sc_pos fen]

        # check if player's move is a direct mate : no need to wait for engine analysis in this case
        set move_done [sc_game info previousMove]
        if { [string index $move_done end] == "#"} { ::tactics::exSolved; return }

        # if the engine is still analyzing, wait the end of it
        if {$analysisEngine(analyzeMode)} { vwait ::tactics::analysisEngine(analyzeMode) }

        if {![winfo exists .tacticsWin]} { return }

        if {[sc_pos fen] != $::tactics::prevFen  && [sc_pos isAt start]} {
            ::tactics::abnormalContinuation
            return
        }

        # the player moved and analysis is over : check if his move was as good as expected
        set prevScore $analysisEngine(score)
        set prevLine $analysisEngine(moves)
        set prevPly $analysisEngine(mateply)
        ::tactics::startAnalyze

        # now wait for the end of analyzis
        if {[sc_pos fen] != $::tactics::prevFen  && [sc_pos isAt start]} {
            ::tactics::abnormalContinuation
            return
        }

        # compare results
        set res [::tactics::foundBestLine]
        if {  $res != ""} {
            tk_messageBox -title "Scid" -icon info -type ok -message "$::tr(BestSolutionNotFound)\n$res"
            # take back last move so restore engine status
            set analysisEngine(score) $prevScore
            set analysisEngine(moves) $prevLine
            set analysisEngine(mateply) $prevPly
            sc_game tags set -site $::tactics::failed
            sc_move back
            updateBoard -pgn
            set ::tactics::prevFen [sc_pos fen]
        } else  {
            catch { sc_move addSan $nextEngineMove }
            set ::tactics::prevFen [sc_pos fen]
            updateBoard -pgn
            if { ! $::tactics::matePending } {
                setInfoEngine $::tr(GoodMove) green
                sc_game tags set -site $::tactics::solved
                sc_game save [sc_game number]
            }
        }

        after 1000 ::tactics::mainLoop
    }
    ################################################################################
    # Returns "" if the user played the best line, otherwise an explanation about the missed move :
    # - guessed the same next move as engine
    # - mate found in the minimal number of moves
    # - combination's score is close enough (within 0.5 point)
    ################################################################################
    proc foundBestLine {} {
        global ::tactics::analysisEngine ::tactics::prevScore ::tactics::prevPly ::tactics::prevLine ::tactics::nextEngineMove ::tactics::matePending
        set score $analysisEngine(score)
        set line $analysisEngine(moves)
        set ply $analysisEngine(mateply)

        set nextEngineMove [ lindex [ split $line ] 0 ]

        # check if the player played the same move predicted by engine
        set prevBestMove [ lindex [ split $prevLine ] 1 ]
        if { [sc_game info previousMoveUCI] == $prevBestMove} {
            return ""
        }

        # Case of mate
        if { $prevPly != 0 } {
            set matePending 1
            # Engine found a mate, search in how many plies
            if { ([sc_pos side] == "black" && $ply < 0 && $ply > $prevPly) || \
                 ([sc_pos side] == "white" && $ply < 0 && $ply > $prevPly) \
                     || $::tactics::winWonGame } {
                return ""
            } else  {
                return $::tr(ShorterMateExists)
            }
        } else  {
            # no mate case
            set matePending 0
            set threshold 0.5
            if {$::tactics::winWonGame} {
                # Only alert when the advantage clearly changes side
                if {[sc_pos side] == "white" && $prevScore < 0 && $score >= $threshold  || \
                            [sc_pos side] == "black" &&  $prevScore >= 0 && $score < [expr 0 - $threshold]  } {
                    return "$::tr(ScorePlayed) $score\n$::tr(Expected) $prevScore"
                } else  {
                    return ""
                }
            }
            if {[ expr abs($prevScore) ] > 3.0 } { set threshold 1.0 }
            if {[ expr abs($prevScore) ] > 5.0 } { set threshold 1.5 }
            # the player moved : score is from opponent side
            set delta [expr abs($score - $prevScore)]
            if { $delta < $threshold } {
                return ""
            } else  {
                return "$::tr(ScorePlayed) $score\n$::tr(Expected) $prevScore"
            }
        }
    }
    ################################################################################
    # Loads a base bundled with Scid (in ./bases directory)
    ################################################################################
    proc loadBase { name } {
        global ::tactics::baseId ::tactics::filter
        lassign [::file::OpenOrSwitch $name] err
        if {$err} { return $err }

        set baseId $::curr_db
        #TODO:
        #set filter [sc_filter new $baseId]
        set filter dbfilter
        sc_filter search $baseId $filter header -filter RESET -flag S -flag| T -site! "\"$::tactics::solved\""
        ::notify::filter $baseId $filter
        return 0
    }
    ################################################################################
    ## resetValues
    #   Resets global data.
    ################################################################################
    proc resetValues {} {
        set ::tactics::prevScore 0
        set ::tactics::prevPly 0
        set ::tactics::prevLine ""
        set ::tactics::nextEngineMove ""
        set ::tactics::matePending 0
        set ::tactics::showSolution 0
        set ::tactics::prevFen ""
        toggleSolution
    }
    ################################################################################
    #
    ################################################################################
    proc setInfoEngine { s { color linen } } {
        set ::tactics::infoEngineLabel $s
        .tacticsWin.f1.labelInfo configure -background $color
    }

    # ======================================================================
    # startAnalyzeMode:
    #   Put the engine in analyze mode
    # ======================================================================
    proc startAnalyze { } {
        global ::tactics::analysisEngine ::tactics::analysisTime
        setInfoEngine "$::tr(Thinking) ..." PaleVioletRed

        # Check that the engine has not already had analyze mode started:
        if {$analysisEngine(analyzeMode)} {
            ::engine::send tacticEngine StopGo
        }

        set analysisEngine(analyzeMode) 1
        ::engine::send tacticEngine Go [list [sc_game UCI_currentPos] [list "movetime" $::tactics::analysisTime]]
        vwait ::analysisEngine(move_done)
        if {[winfo exists .tacticsWin]} {
            setInfoEngine $::tr(AnalyzeDone) PaleGreen3
        }
        set analysisEngine(analyzeMode) 0
    }
    # ======================================================================
    # stopAnalyzeMode:
    #   Stop the engine analyze mode
    # ======================================================================
    proc stopAnalyze { } {
        global ::tactics::analysisEngine
        # Check that the engine has already had analyze mode started:
        if {!$analysisEngine(analyzeMode)} { return }

        ::engine::send tacticEngine StopGo
        set analysisEngine(analyzeMode) 0
        if {[winfo exists .tacticsWin]} {
            setInfoEngine $::tr(AnalyzeDone) PaleGreen3
        }
    }

}

###
### End of file: tactics.tcl
###
