(ns ^:no-doc com.timezynk.mongo.methods.collation
  (:import
   [com.mongodb.client.model
    Collation CollationAlternate CollationCaseFirst CollationMaxVariable CollationStrength]))

(defn collation-method [{:keys [alternate backwards case-level case-first locale
                                max-variable normalization numeric-ordering strength]}]
  (cond-> (Collation/builder)
    alternate        (.collationAlternate (case alternate
                                            :non-ignorable CollationAlternate/NON_IGNORABLE
                                            :shifted       CollationAlternate/SHIFTED))
    backwards        (.backwards backwards)
    case-first       (.collationCaseFirst (case case-first
                                            :lower CollationCaseFirst/LOWER
                                            :off   CollationCaseFirst/OFF
                                            :upper CollationCaseFirst/UPPER))
    case-level       (.caseLevel case-level)
    locale           (.locale locale)
    max-variable     (.collationMaxVariable (case max-variable
                                              :punct CollationMaxVariable/PUNCT
                                              :space CollationMaxVariable/SPACE))
    normalization    (.normalization normalization)
    numeric-ordering (.numericOrdering numeric-ordering)
    strength         (.collationStrength (case strength
                                           :identical  CollationStrength/IDENTICAL
                                           :primary    CollationStrength/PRIMARY
                                           :quaternary CollationStrength/QUATERNARY
                                           :secondary  CollationStrength/SECONDARY
                                           :tertiary   CollationStrength/TERTIARY))))
