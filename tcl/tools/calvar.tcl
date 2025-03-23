###
### calvar.tcl: part of Scid.
### Copyright (C) 2007  Pascal Georges
###
################################################################################
# The number used for the engine playing a serious game is 4
################################################################################

namespace eval calvar {
  # DEBUG
  set ::uci::uciInfo(log_stdout4) 0

  array set engineListBox {}
  set thinkingTimePerLine 3
  set thinkingTimePosition 10
  set currentLine 1
  set currentListMoves {}
  set engineName ""
  # each line begins with a list of moves, a nag code and ends with FEN
  set lines {}
  set analysisQueue {}

  # contains multipv analysis of the position, to see if the user considered all important lines
  set initPosAnalysis {}

  set working 0
  set midmove ""

  set afterIdPosition 0
  set afterIdLine 0

  trace add variable ::calvar::working write { ::calvar::traceWorking }
  ################################################################################
  #
  ################################################################################
  proc traceWorking {a b c} {
    set widget .calvarWin.fCommand.bDone
    if {$::calvar::working} {
      $widget configure -state disabled
    } else {
      $widget configure -state normal
    }
  }
  ################################################################################
  #
  ################################################################################
  proc reset {} {
    set currentLine 1
    set currentListMoves {}
    set lines {}
    set working 0
    set analysisQueue {}
    set ::calvar::initPosAnalysis {}
    if {[winfo exists .calvarWin]} {
      .calvarWin.fText.t delete 1.0 end
    }
  }
  ################################################################################
  #
  ################################################################################
  proc config {} {

    # check if game window is already opened. If yes abort previous game
    set w ".calvarWin"
    if {[winfo exists $w]} {
      focus .calvarWin
      return
    }

    set w ".configCalvarWin"
    if {[winfo exists $w]} {
      focus $w
      return
    }

    win::createDialog $w
    wm title $w [::tr "ConfigureCalvar"]

    bind $w <F1> { helpWindow CalVar }
    setWinLocation $w

    # builds the list of UCI engines
    ttk::frame $w.fengines
    ::engineNoWin::createEngineOptionsFrame $w calcvarEngine ::calvar::engineName 5 ::calvar::eng_messages
    pack $w.calcvarEngine -in $w.fengines -side top -pady 5 -anchor w -padx 4
    grid $w.fengines -row 0 -column 0 -pady { 0 10 } -sticky nswe -padx { 0 10 }
    # parameters setting
    set f $w.parameters
    ttk::frame $w.parameters
    grid $f -row 1 -column 0 -sticky nswe -padx { 0 10 }
    ttk::label $f.lTime -text $::tr(SecondsPerMove)
    ttk::spinbox $f.sbTime -width 3 -textvariable ::calvar::thinkingTimePerLine -from 1 -to 120 -increment 1 -validate all -validatecommand { regexp {^[0-9]+$} %P }
    ttk::label $f.lTime2 -text "Position thinking time"
    ttk::spinbox $f.sbTime2 -width 3 -textvariable ::calvar::thinkingTimePosition -from 1 -to 300 -increment 1 -validate all -validatecommand { regexp {^[0-9]+$} %P }
    grid $f.lTime -column 0 -row 0 -sticky w
    grid $f.sbTime -column 1 -row 0 -padx 10 -pady 5
    grid $f.lTime2 -column 0 -row 1 -sticky w
    grid $f.sbTime2 -column 1 -row 1 -padx 10

    ttk::frame $w.fbuttons
    grid $w.fbuttons -row 2 -column 0 -sticky se -padx { 0 10 }
    ttk::button $w.fbuttons.start -text Start -command {
      focus .
      set callback [list ::calvar::eng_messages calvarEngine nop]
      if { [::engineNoWin::initEngine calvarEngine $::calvar::engineName $callback "MultiPV 10"] } {
          destroy .configCalvarWin
          ::calvar::start calvarEngine
      }
    }
    ttk::button $w.fbuttons.cancel -textvar ::tr(Cancel) -command "focus .; destroy $w"

    packdlgbuttons $w.fbuttons.cancel $w.fbuttons.start

    bind $w <Escape> { .configCalvarWin.fbuttons.cancel invoke }
    bind $w <Return> { .configCalvarWin.fbuttons.start invoke }
    bind $w <Destroy> ""
    bind $w <Configure> "recordWinSize $w"
    wm resizable $w 0 0
  }

  proc ::calvar::eng_messages {id w msg} {
      lassign $msg msgType msgData
      switch $msgType {
          "InfoConfig" {
              if { ! [winfo exists $w] } { return }
              set msgData [lindex $msgData 2]
              ::engineNoWin::initEngineOptions $id $w $msgData
          }
          "InfoPV" {
              # no coach engine then use score from playing engine
              lassign $msgData multipv depth seldepth nodes nps hashfull tbhits time score score_type score_wdl pv
              set ::calvar::data(pv$multipv) [list $depth [expr $score / 100.0] $pv]
          }
          "InfoBestMove" {
              lassign $msgData ::calvar::data(bestmove) ponder ::calvar::data(ponder)
          }
          "InfoDisconnected" {
              lassign $msgData errorMsg
              if {$errorMsg eq ""} { set errorMsg "The connection with the engine terminated unexpectedly." }
              tk_messageBox -icon warning -type ok -parent . -message $errorMsg
              ::sergame::abortGame
          }
      }
  }

  ################################################################################
  #
  ################################################################################
  proc start { engine } {

    ::calvar::reset

    set w ".calvarWin"
    if {[winfo exists $w]} {
      focus .calvarWin
      return
    }
    createToplevel $w
    applyThemeColor_background $w
    ::setTitle $w [::tr "Calvar"]
    bind $w <F1> { helpWindow CalVar }

    set f $w.fNag
    ttk::frame $f
    set i 0
    foreach nag { "=" "+=" "+/-" "+-" "=+" "-/+" "-+" } {
      ttk::button $f.nag$i -text $nag -command "::calvar::nag $nag" -width 3
      pack $f.nag$i -side left
      incr i
    }

    set f $w.fText
    ttk::frame $f
    text $f.t -height 12 -width 50
    applyThemeStyle Treeview $f.t
    pack $f.t -expand 1 -fill both

    set f $w.fPieces
    ttk::frame $f
    ttk::label $f.lPromo -text "Promotion"
    pack $f.lPromo -side left
    foreach piece { "q" "r" "b" "n" } {
      ttk::button $f.p$piece -image w${piece}20 -command "::calvar::promo $piece"
      pack $f.p$piece -side left
    }

    set f $w.fCommand
    ttk::frame $f
    ttk::button $f.bDone -text [::tr "DoneWithPosition"] -command ::calvar::positionDone
    pack $f.bDone

    set f $w.fbuttons
    ttk::frame $f
    ttk::button $w.fbuttons.stop -textvar ::tr(Stop) -command "::calvar::stop"
    pack $w.fbuttons.stop -expand yes -side left -padx 20 -pady 2

    pack $w.fNag $w.fText $w.fPieces $w.fCommand $w.fbuttons -side top -fill both -pady { 5 5 }

    bind $w <Escape> { .calvarWin.fbuttons.stop invoke }
    bind $w <Destroy> ""
    bind $w <Configure> "recordWinSize $w"
    wm minsize $w 45 0

    set ::calvar::suggestMoves_old $::suggestMoves
    set ::calvar::hideNextMove_old $::gameInfo(hideNextMove)

    set ::suggestMoves 0
    set ::gameInfo(hideNextMove) 1
    updateBoard

    # fill initPosAnalysis for the current position
    ::calvar::startAnalyze "" "" [sc_pos fen]

    ::createToplevelFinalize $w
  }
  ################################################################################
  #
  ################################################################################
  proc stop { } {
    after cancel $::calvar::afterIdPosition
    after cancel $::calvar::afterIdLine
    ::engine::close calvarEngine
    unset ::enginewin::engConfig_calvarEngine
    focus .
    destroy .calvarWin
    set ::suggestMoves $::calvar::suggestMoves_old
    set ::gameInfo(hideNextMove) $::calvar::hideNextMove_old
    updateBoard
  }

  ################################################################################
  #
  ################################################################################
  proc pressSquare { sq } {
    global ::calvar::midmove

    set sansq [::board::san $sq]
    if {$midmove == ""} {
      set midmove $sansq
    } else {
      lappend ::calvar::currentListMoves "$midmove$sansq"
      set midmove ""
    }
    set tmp " "
    if {$midmove == ""} {
      set tmp "-"
    }
    .calvarWin.fText.t insert "$::calvar::currentLine.end" "$tmp$sansq"
  }
  ################################################################################
  #
  ################################################################################
  proc promo { piece } {
    if { [llength $::calvar::currentListMoves] == 0 } { return }

    set tmp [lindex $::calvar::currentListMoves end]
    set tmp "$tmp$piece"
    lset ::calvar::currentListMoves end $tmp
    .calvarWin.fText.t insert end "$piece"
  }
  ################################################################################
  # This will end a line, and start engine computation
  ################################################################################
  proc nag { n } {
    if { $::calvar::midmove ne "" } {
        tk_messageBox -type ok -message "Move incomplete." -parent .main -icon info
        return
    }
    .calvarWin.fText.t insert "$::calvar::currentLine.end" " $n\n"
    set newline [list $::calvar::currentListMoves $n [sc_pos fen]]
    lappend ::calvar::lines $newline
    incr ::calvar::currentLine
    addLineToCompute $newline
    set ::calvar::currentListMoves {}
  }
  ################################################################################
  #
  ################################################################################
  proc addLineToCompute {line } {
    global ::calvar::analysisQueue
    if {$line != ""} {
      lappend analysisQueue $line
    }
    if { $::calvar::working } { set ::calvar::afterIdLine [after 1000 {::calvar::addLineToCompute ""}]; return }

    if { [llength $analysisQueue] != 0 } {
      set line [lindex $analysisQueue 0]
      set analysisQueue [lreplace $analysisQueue 0 0]
      startAnalyze [lindex $line 0] [lindex $line 1] [lindex $line 2]
    }
  }
  ################################################################################
  # we suppose FEN has not changed !
  ################################################################################
  proc handleResult {moves nag fen } {
    set firstmove [lindex $moves 0]

    set pv [ lindex $::analysis(multiPV) 0 ]
    if { [ llength $pv ] >=3 } {
      set engmoves [lindex $pv 2]
      # score is computed for the opposite side, so invert it
      set engscore [expr - 1.0 * [lindex $pv 1]]
      set engdepth [lindex $pv 0]
      addVar $moves "$firstmove $engmoves" $nag $engscore
    } else  {
      puts "Error pv = $pv"
    }
  }
  ################################################################################
  # will add a variation at current position.
  # Try to merge the variation with an existing one.
  ################################################################################
  proc addVar {usermoves engmoves nag engscore} {
    # Cannot add a variation to an empty variation:
    if {[sc_pos isAt vstart]  &&  [sc_pos isAt vend]} {
      # enter the first move as dummy variation
      sc_move addSan [lindex $engmoves 0]
      sc_move back
    }

    set repeat_move ""
    # If at the end of the game or a variation, repeat previous move
    if {[sc_pos isAt vend] && ![sc_pos isAt vstart]} {
      set repeat_move [sc_game info previousMoveNT]
      sc_move back
    }

    # first enter the user moves
    sc_var create
    if {$repeat_move != ""} {sc_move addSan $repeat_move}
    if { [catch { sc_move addSan $usermoves }] } {
        sc_pos setComment " error in user moves $usermoves"
    }

    sc_pos addNag $nag

    # now enter the engine moves
    while {![sc_pos isAt vstart] } {sc_move back}
    if {$repeat_move != ""} {sc_move forward}
    sc_var create
    sc_pos setComment "$::calvar::engineName : \[%eval $engscore\]"
    if { [catch { sc_move addSan $engmoves }] } {
        sc_pos setComment "Wrong first user move. $::calvar::engineName : $engmoves ignored"
    }
    sc_var exit
    sc_var exit

    if {$repeat_move != ""} {sc_move forward}

    updateBoard -pgn
  }
  ################################################################################
  # will add a variation at current position.
  # Try to merge the variation with an existing one.
  ################################################################################
  proc addMissedLine {moves score depth} {
    # Cannot add a variation to an empty variation:
    if {[sc_pos isAt vstart]  &&  [sc_pos isAt vend]} {
      # enter the first move as dummy variation
      sc_move addSan [lindex $moves 0]
      sc_move back
    }

    set repeat_move ""
    # If at the end of the game or a variation, repeat previous move
    if {[sc_pos isAt vend] && ![sc_pos isAt vstart]} {
      set repeat_move [sc_game info previousMoveNT]
      sc_move back
    }

    sc_var create
    if {$repeat_move != ""} {sc_move addSan $repeat_move}
    sc_pos setComment "Missed line ($depth) \[%eval $score\]"
    sc_move addSan $moves
    sc_var exit
    if {$repeat_move != ""} { sc_move forward }

    updateBoard -pgn
  }
  ################################################################################
  # The user stops entering var, check he founds all important ones.
  # All the moves that the user did not consider with a score better than the first best
  # move entered by the user should be pointed out.
  ################################################################################
  proc positionDone {} {
    global ::calvar::initPosAnalysis ::calvar::lines

    ################################################################################
    proc isPresent { engmoves } {
      global ::calvar::lines
      set res 0
      set firsteng [lindex $engmoves 0]
      foreach userLine $::calvar::lines {
        set usermoves [lindex $userLine 0]
        set firstuser [lindex $usermoves 0]
        if {$firstuser == $firsteng} { return 1 }
      }
      return 0
    }

    ################################################################################
    foreach pv $::calvar::initPosAnalysis {
      set engmoves [lindex $pv 2]
      set engscore [lindex $pv 1]
      set engdepth [lindex $pv 0]
      if { ! [isPresent $engmoves] } {
        addMissedLine $engmoves $engscore $engdepth
      } else {
        # the user considered at least one line (skip those that are below)
        break
      }
    }
    ::calvar::reset
  }
  ################################################################################
  # startAnalyze:
  # Put the engine in analyze mode and ponder on the first move entered by the user to see
  # if the line's evaluation is coherent
  ################################################################################
  proc startAnalyze {moves nag fen } {
    # Check that the engine has not already had analyze mode started:
    if { [llength $moves] > 0 } {
      set time [expr $::calvar::thinkingTimePerLine * 1000]
      set pos "position fen $fen moves [lindex $moves 0]"
    } else {
      set time [expr $::calvar::thinkingTimePosition * 1000]
      set pos "position fen $fen"
    }
    set ::calvar::working 1
    ::engine::send calvarEngine Go [list $pos "movetime $time"]
    vwait ::calvar::data(bestmove)
    set ::calvar::working 0
    set ::analysis(multiPV) {}
    set i 1
    while { [info exists ::calvar::data(pv$i)] } {
        lappend ::analysis(multiPV) $::calvar::data(pv$i)
        unset ::calvar::data(pv$i)
        incr i
    }
    if { [llength $moves] > 0 } {
      handleResult $moves $nag $fen
    } else {
        set ::calvar::initPosAnalysis $::analysis(multiPV)
    }
    addLineToCompute ""

  }
}
###
### End of file: calvar.tcl
###
