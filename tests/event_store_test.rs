use tempfile::tempdir;
use uuid::Uuid;
use dcbsd::api::{DCBAppendCondition, DCBQuery, DCBEvent, EventStoreError, DCBQueryItem, DCBEventStoreAPI, DCBEventStoreAPIExt};
use dcbsd::store::EventStore;
// Import the EventStore and related types from the main crate

#[test]
fn test_direct_event_store() {
    let temp_dir = tempdir().unwrap();
    let event_store = EventStore::new(temp_dir.path()).unwrap();
    run_event_store_test(&event_store);
}


// Helper function to run the test with a given EventStoreApi implementation
pub fn run_event_store_test<T: DCBEventStoreAPI + DCBEventStoreAPIExt>(event_store: &T) {

    // Read all, expect no results.
    let (result, head) = event_store.read_as_tuple(None, None, None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Append one event.
    let event1 = DCBEvent {
        event_type: "type1".to_string(),
        data: b"data1".to_vec(),
        tags: vec!["tagX".to_string()],
    };
    let position = event_store.append(vec![event1.clone()], None).unwrap();

    // Check the returned position is 1.
    assert_eq!(1, position);

    // Read all, expect one event.
    let (result, head) = event_store.read_as_tuple(None, None, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read all after 1, expect no events.
    let (result, head) = event_store.read_as_tuple(None, Some(1), None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read all limit 1, expect one event.
    let (result, head) = event_store.read_as_tuple(None, None, Some(1)).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read all limit 0, expect no events (and head is None).
    let (result, head) = event_store.read_as_tuple(None, None, Some(0)).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read events with type1, expect 1 event.
    let query_type1 = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type1".to_string()],
            tags: vec![],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type1.clone()), None, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read events with type2, expect no events.
    let query_type2 = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type2".to_string()],
            tags: vec![],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type2.clone()), None, None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read events with tagX, expect one event.
    let query_tag_x = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagX".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_tag_x.clone()), None, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read events with tagY, expect no events.
    let query_tag_y = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagY".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_tag_y.clone()), None, None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read events with type1 and tagX, expect one event.
    let query_type1_tag_x = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type1".to_string()],
            tags: vec!["tagX".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type1_tag_x.clone()), None, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(1), head);

    // Read events with type1 and tagY, expect no events.
    let query_type1_tag_y = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type1".to_string()],
            tags: vec!["tagY".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type1_tag_y), None, None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read events with type2 and tagX, expect no events.
    let query_type2_tag_x = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type2".to_string()],
            tags: vec!["tagX".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type2_tag_x), None, None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Append two more events.
    let event2 = DCBEvent {
        event_type: "type2".to_string(),
        data: b"data2".to_vec(),
        tags: vec!["tagA".to_string(), "tagB".to_string()],
    };
    let event3 = DCBEvent {
        event_type: "type3".to_string(),
        data: b"data3".to_vec(),
        tags: vec!["tagA".to_string(), "tagC".to_string()],
    };
    let position = event_store.append(vec![event2.clone(), event3.clone()], None).unwrap();

    // Check the returned position is 3
    assert_eq!(3, position);

    // Read all, expect 3 events (in ascending order).
    let (result, head) = event_store.read_as_tuple(None, None, None).unwrap();
    assert_eq!(3, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(event3.data, result[2].event.data);
    assert_eq!(Some(3), head);

    // Read all after 1, expect two events.
    let (result, head) = event_store.read_as_tuple(None, Some(1), None).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Read all after 2, expect one event.
    let (result, head) = event_store.read_as_tuple(None, Some(2), None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read all after 1, limit 1, expect one event.
    let (result, head) = event_store.read_as_tuple(None, Some(1), Some(1)).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(2), head);

    // Read all after 10, limit 10, expect zero events.
    let (result, head) = event_store.read_as_tuple(None, Some(10), Some(10)).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read type1 after 1, expect no events.
    let (result, head) = event_store.read_as_tuple(Some(query_type1.clone()), Some(1), None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Read tagX after 1, expect no events.
    let (result, head) = event_store.read_as_tuple(Some(query_tag_x.clone()), Some(1), None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Read tagX after 1, limit 1 expect no events.
    let (result, head) = event_store.read_as_tuple(Some(query_tag_x.clone()), Some(1), Some(1)).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read type1 and tagX after 1, expect no events.
    let (result, head) = event_store.read_as_tuple(Some(query_type1_tag_x.clone()), Some(1), None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Read events with tagA, expect two events.
    let query_tag_a = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagA".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_tag_a.clone()), None, None).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Read events with tagA and tagB, expect one event.
    let query_tag_a_and_b = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagA".to_string(), "tagB".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_tag_a_and_b.clone()), None, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with tagB or tagC, expect two events.
    let query_tag_b_or_c = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagB".to_string()],
            },
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagC".to_string()],
            },
        ],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_tag_b_or_c.clone()), None, None).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Read events with tagX or tagY, expect one event.
    let query_tag_x_or_y = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagX".to_string()],
            },
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagY".to_string()],
            },
        ],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_tag_x_or_y.clone()), None, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with type2 and tagA, expect one event.
    let query_type2_tag_a = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type2".to_string()],
            tags: vec!["tagA".to_string()],
        }],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type2_tag_a.clone()), None, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with type2 and tagA after 2, expect no events.
    let (result, head) = event_store.read_as_tuple(Some(query_type2_tag_a.clone()), Some(2), None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Read events with type2 and tagB, or with type3 and tagC, expect two events.
    let query_type2_tag_b_or_type3_tagc = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec!["type2".to_string()],
                tags: vec!["tagB".to_string()],
            },
            DCBQueryItem {
                types: vec!["type3".to_string()],
                tags: vec!["tagC".to_string()],
            },
        ],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type2_tag_b_or_type3_tagc.clone()), None, None).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Repeat with query items in different order, expect events in ascending order.
    let query_type3_tag_c_or_type2_tag_b = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec!["type3".to_string()],
                tags: vec!["tagC".to_string()],
            },
            DCBQueryItem {
                types: vec!["type2".to_string()],
                tags: vec!["tagB".to_string()],
            },
        ],
    };
    let (result, head) = event_store.read_as_tuple(Some(query_type3_tag_c_or_type2_tag_b), None, None).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Append must fail if recorded events match condition.
    let event4 = DCBEvent {
        event_type: "type4".to_string(),
        data: b"data4".to_vec(),
        tags: vec![],
    };

    // Fail because condition matches all.
    let new = vec![event4.clone()];
    let result = event_store.append(new.clone(), Some(DCBAppendCondition::default()));
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches all after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: DCBQuery::default(),
            after: Some(1),
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches type1.
    let result = event_store.append(new.clone(), Some(DCBAppendCondition {
        fail_if_events_match: query_type1.clone(),
        after: None,
    }));
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches type2 after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type2.clone(),
            after: Some(1),
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches tagX.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_x.clone(),
            after: None,
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches tagA after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_a.clone(),
            after: Some(1),
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches type1 and tagX.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type1_tag_x.clone(),
            after: None,
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches type2 and tagA after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type2_tag_a.clone(),
            after: Some(1),
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches tagA and tagB.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_a_and_b.clone(),
            after: None,
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches tagB or tagC.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_b_or_c.clone(),
            after: None,
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches tagX or tagY.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_x_or_y.clone(),
            after: None,
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Fail because condition matches with type2 and tagB, or with type3 and tagC.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type2_tag_b_or_type3_tagc.clone(),
            after: None,
        }),
    );
    assert!(matches!(result, Err(EventStoreError::IntegrityError)));

    // Can append after 3.
    let position = event_store.append(new.clone(), None).unwrap();
    assert_eq!(4, position);

    // Can append match type_n.
    let query_type_n = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["typeN".to_string()],
            tags: vec![],
        }],
    };
    let position = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type_n,
            after: None,
        }),
    ).unwrap();
    assert_eq!(5, position);

    // Can append match tagY.
    let position = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_y.clone(),
            after: None,
        }),
    ).unwrap();
    assert_eq!(6, position);

    // Can append match type1 after 1.
    let position = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type1.clone(),
            after: Some(1),
        }),
    ).unwrap();
    assert_eq!(7, position);

    // Can append match tagX after 1.
    let position = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_x.clone(),
            after: Some(1),
        }),
    ).unwrap();
    assert_eq!(8, position);

    // Can append match type1 and tagX after 1.
    let position = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type1_tag_x.clone(),
            after: Some(1),
        }),
    ).unwrap();
    assert_eq!(9, position);

    // Can append match tagX, after 1.
    let position = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_x.clone(),
            after: Some(1),
        }),
    ).unwrap();
    assert_eq!(10, position);

    // Check it works with course subscription consistency boundaries and events.
    let student_id = format!("student1-{}", Uuid::new_v4());
    let student_registered = DCBEvent {
        event_type: "StudentRegistered".to_string(),
        data: format!(r#"{{"name": "Student1", "max_courses": 10}}"#).into_bytes(),
        tags: vec![student_id.clone()],
    };

    let course_id = format!("course1-{}", Uuid::new_v4());
    let course_registered = DCBEvent {
        event_type: "CourseRegistered".to_string(),
        data: format!(r#"{{"name": "Course1", "places": 10}}"#).into_bytes(),
        tags: vec![course_id.clone()],
    };

    let student_joined_course = DCBEvent {
        event_type: "StudentJoinedCourse".to_string(),
        data: format!(r#"{{"student_id": "{}", "course_id": "{}"}}"#, student_id, course_id).into_bytes(),
        tags: vec![course_id.clone(), student_id.clone()],
    };

    let _position = event_store.append(
        vec![student_registered.clone()],
        Some(DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec!["StudentRegistered".to_string()],
                    tags: student_registered.tags.clone(),
                }],
            },
            after: Some(3),
        }),
    ).unwrap();

    let _position = event_store.append(
        vec![course_registered.clone()],
        Some(DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: course_registered.tags.clone(),
                }],
            },
            after: Some(3),
        }),
    ).unwrap();

    let _position = event_store.append(
        vec![student_joined_course.clone()],
        Some(DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: student_joined_course.tags.clone(),
                }],
            },
            after: Some(3),
        }),
    ).unwrap();

    let (result, head) = event_store.read_as_tuple(None, None, None).unwrap();
    assert_eq!(13, result.len());
    assert_eq!(result[10].event.event_type, student_registered.event_type);
    assert_eq!(result[11].event.event_type, course_registered.event_type);
    assert_eq!(result[12].event.event_type, student_joined_course.event_type);
    assert_eq!(result[10].event.data, student_registered.data);
    assert_eq!(result[11].event.data, course_registered.data);
    assert_eq!(result[12].event.data, student_joined_course.data);
    assert_eq!(result[10].event.tags, student_registered.tags);
    assert_eq!(result[11].event.tags, course_registered.tags);
    assert_eq!(result[12].event.tags, student_joined_course.tags);
    assert_eq!(Some(13), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![student_id.clone()],
            }],
        }),
        None,
        None,
    ).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(Some(13), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![course_id.clone()],
            }],
        }),
        None,
        None,
    ).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(Some(13), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![student_joined_course.tags[0].clone(), student_joined_course.tags[1].clone()],
            }],
        }),
        None,
        None,
    ).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![student_id.clone()],
            }],
        }),
        Some(2),
        None,
    ).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(Some(13), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![course_id.clone()],
            }],
        }),
        Some(2),
        None,
    ).unwrap();
    assert_eq!(2, result.len());
    assert_eq!(Some(13), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![student_joined_course.tags[0].clone(), student_joined_course.tags[1].clone()],
            }],
        }),
        Some(2),
        None,
    ).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![student_id.clone()],
            }],
        }),
        Some(2),
        Some(1),
    ).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(11), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![course_id.clone()],
            }],
        }),
        Some(2),
        Some(1),
    ).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(12), head);

    let (result, head) = event_store.read_as_tuple(
        Some(DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![student_joined_course.tags[0].clone(), student_joined_course.tags[1].clone()],
            }],
        }),
        Some(2),
        Some(1),
    ).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    let consistency_boundary = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec!["StudentRegistered".to_string(), "StudentJoinedCourse".to_string()],
                tags: vec![student_id.clone()],
            },
            DCBQueryItem {
                types: vec!["CourseRegistered".to_string(), "StudentJoinedCourse".to_string()],
                tags: vec![course_id.clone()],
            },
        ],
    };
    let (result, head) = event_store.read_as_tuple(Some(consistency_boundary), None, None).unwrap();
    assert_eq!(3, result.len());
    assert_eq!(Some(13), head);
}
