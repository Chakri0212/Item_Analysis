
-- while very similar to the AtomACT query, there are enough differences
-- that you want to be careful when altering them


-- using SELECT DISTINCT because Atom occasionally sends multiple start events
-- also they somehow are able to reuse IDs on content items?

SELECT DISTINCT 'Atom' AS source_system
	, '200' AS history_db_id
	, AF.student_id
	, III.sequence_id AS activity_id
	, III.position AS item_position
	, SEC.section_title
	, III.position AS item_section_position
	, III.item_id AS content_item_id
	, III.item_name AS content_item_name
	, CI.interaction_type
	, CASE 
		WHEN III_SUBMITTED.time_test_total IS NOT NULL THEN III_SUBMITTED.time_test_total
		ELSE III_FINAL.time_test_total END AS milliseconds_used
	, III.scorable AS is_scored
	, III_SUBMITTED.item_scored_response AS scored_response
	
	-- only works for multiple choice items
	-- text entry will show up as NULL
	--, III_SUBMITTED.choice_native_position AS raw_response 
	
	
	, CASE 
		WHEN III_SUBMITTED.item_id IS NOT NULL THEN 'responded'
		WHEN milliseconds_used > 100 THEN 'omitted'
		ELSE 'not-reached' END AS item_status
	, FT.field_test
	
	
	-- needs to be redone for some item types
	, III_SUBMITTED.item_score
	
	
	, date_trunc('second', III_SUBMITTED.event_timestamp) AS item_submitted_timestamp
	
	, III_FINAL.is_reviewed
	, III_FINAL.time_review_total AS milliseconds_review_total
	, III_FINAL.time_explanation_total AS milliseconds_review_explanation

	-- temporary hack so we can:
	--    fix the interaction order problem (KDE-304)
	--    extract hotspot and text-entry responses
	-- this column does not need to be added to other response queries
	-- because the application code removes it when found
	--, III_SUBMITTED.interaction_responses

	
FROM atom_activity.section SEC

	JOIN atom_activity.sequence SEQ
		ON SEC.sequence_id IN ({activity_id})
		AND SEQ.sequence_id = SEC.sequence_id
		AND SEQ.event_type = 'start'

	-- just so I can get the student ID
	JOIN student_performance.activity_fact AF
		ON AF.sequence_id = SEQ.sequence_id
		AND AF.source_system = 'Atom'
		AND AF.sequence_id in ({activity_id})
		
	-- get initial item info
	JOIN atom_activity.item_instance_interaction III
		ON III.sequence_id = SEC.sequence_id
		AND III.item_name IN ({rational_items})
		
		-- sections are created from start and start_section events
		-- currently only the start event has a title or name
		-- so fine for now, but once an adaptive section test exists (GRE, custom GMAT?)
		-- Atom had better start naming those sections
		-- unless they decide to prename all possible sections in the start event
		-- which is a terrible idea so yeah they'll probably do that
		AND SEC.section_title IS NOT NULL
		
		AND III.event_type = 'add_content'
		AND III.section_id = SEC.section_id
		

	-- get last submit event for this item
	-- because - haha - sometimes Atom sends bonus nothing events afterwards
	LEFT JOIN (
		SELECT III_TEMP.*
			, row_number() over (
			PARTITION BY parent_instance_id, item_id
			ORDER BY event_timestamp DESC
		) AS rowNumber
		FROM atom_activity.item_instance_interaction III_TEMP
		WHERE III_TEMP.sequence_id IN ({activity_id})
			AND event_type = 'interact'
			AND III_TEMP.is_submitted = 1
			AND III_TEMP.item_name IN ({rational_items})

	)  III_SUBMITTED
		ON III_SUBMITTED.parent_instance_id = III.parent_instance_id				       
		-- account for question sets
		AND III_SUBMITTED.item_id = III.item_id
		AND III_SUBMITTED.rowNumber = 1
		AND SEC.section_id = III_SUBMITTED.section_id


	-- get last event for this item
	LEFT JOIN (
		SELECT III_TEMP.*
			, row_number() over (
			PARTITION BY parent_instance_id, item_id
			ORDER BY event_timestamp DESC
		) AS rowNumber
		FROM atom_activity.item_instance_interaction III_TEMP
		WHERE III_TEMP.sequence_id IN ({activity_id})
		AND III_TEMP.item_name IN ({rational_items})
			
	)  III_FINAL
		ON III_FINAL.sequence_id = III.sequence_id
		AND III_FINAL.parent_instance_id = III.parent_instance_id				       
		-- account for question sets
		AND III_FINAL.item_id = III.item_id
		AND III_FINAL.rowNumber = 1
		
	-- so we can identify field test items
	LEFT JOIN (
		SELECT CTM.item_id, '1' AS field_test
		FROM atom_activity.content_tag_mapping CTM
		WHERE CTM.tag_id IN (
			SELECT tag_id
			FROM atom_activity.content_tag
			WHERE tag_name = 'fieldtest'
		)
	) FT ON FT.item_id = III.item_id
		

	-- so we can get interaction type
	LEFT JOIN atom_activity.content_items CI
		ON CI.item_id = III.item_id
		AND CI.item_type = 'question'
		
		
ORDER BY III.sequence_id	
	, item_position
		