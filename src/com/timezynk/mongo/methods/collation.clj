(ns ^:no-doc com.timezynk.mongo.methods.collation
  (:import
   [com.mongodb.client.model
    Collation CollationAlternate CollationCaseFirst CollationMaxVariable CollationStrength]))

(defn- with-options [result {:keys [alternate backwards? case-first case-level?
                                    max-variable normalization? numeric-ordering? strength]}]
  (cond-> result
    alternate         (.collationAlternate (case alternate
                                            :non-ignorable CollationAlternate/NON_IGNORABLE
                                            :shifted       CollationAlternate/SHIFTED))
    backwards?        (.backwards backwards?)
    case-first        (.collationCaseFirst (case case-first
                                            :lower CollationCaseFirst/LOWER
                                            :off   CollationCaseFirst/OFF
                                            :upper CollationCaseFirst/UPPER))
    case-level?       (.caseLevel case-level?)
    max-variable      (.collationMaxVariable (case max-variable
                                              :punct CollationMaxVariable/PUNCT
                                              :space CollationMaxVariable/SPACE))
    normalization?    (.normalization normalization?)
    numeric-ordering? (.numericOrdering numeric-ordering?)
    strength          (.collationStrength (case strength
                                           :identical  CollationStrength/IDENTICAL
                                           :primary    CollationStrength/PRIMARY
                                           :quaternary CollationStrength/QUATERNARY
                                           :secondary  CollationStrength/SECONDARY
                                           :tertiary   CollationStrength/TERTIARY))))

(defn collation-method [locale options]
  (-> (Collation/builder)
      (.locale locale)
      (with-options options)
      (.build)))
